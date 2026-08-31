
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace OtelTap
{
    /// <summary>
    /// Listens for OTLP telemetry in http/protobuf format on a given port.
    /// Streams it as IAsyncEnumerable.
    /// Optionally prints it into standard output and re-emits it to given receiver endpoints.
    /// </summary>
    public class OtelTapHttpProtobufReceiver : IDisposable
    {
        private ulong? handle;
        private readonly Action<string, Exception?> log = (s, e) => { };

        private const int PollIntervalInMs = 256;

        private readonly CancellationTokenSource pollLoopCts = new CancellationTokenSource();
        private readonly Task tracesPollingTask;
        private readonly Task logsPollingTask;
        private readonly Task metricsPollingTask;

        private ImmutableArray<Channel<Span>> traceSubscribers = ImmutableArray<Channel<Span>>.Empty;
        private ImmutableArray<Channel<LogRecord>> logSubscribers = ImmutableArray<Channel<LogRecord>>.Empty;
        private ImmutableArray<Channel<Metric>> metricSubscribers = ImmutableArray<Channel<Metric>>.Empty;

        private readonly List<(Func<Span, bool> predicate, TaskCompletionSource<Span> completionSource)> traceAwaiters = new();
        private readonly Lock traceAwaitersLock = new();

        private readonly List<(Func<LogRecord, bool> predicate, TaskCompletionSource<LogRecord> completionSource)> logAwaiters = new();
        private readonly Lock logAwaitersLock = new();

        private readonly List<(Func<Metric, bool> predicate, TaskCompletionSource<Metric> completionSource)> metricAwaiters = new();
        private readonly Lock metricAwaitersLock = new();

        private OtelTapHttpProtobufReceiver(ulong handle, Action<string, Exception?>? log)
        {
            this.handle = handle;

            if (log != null)
            {
                this.log = log;
            }

            // Start long-running polling tasks
            var cancelToken = this.pollLoopCts.Token;

            this.tracesPollingTask = Task.Factory.StartNew(
                () => this.PollTraces(cancelToken),
                cancelToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            this.logsPollingTask = Task.Factory.StartNew(
                () => this.PollLogs(cancelToken),
                cancelToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);

            this.metricsPollingTask = Task.Factory.StartNew(
                () => this.PollMetrics(cancelToken),
                cancelToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        ~OtelTapHttpProtobufReceiver()
        {
            this.Dispose();
        }

        /// <summary>
        /// Starts a <see cref="OtelTapHttpProtobufReceiver"/>.
        /// </summary>
        /// <param name="settings">Config settings</param>
        /// <returns>The running <see cref="OtelTapHttpProtobufReceiver"/> instance.</returns>
        /// <exception cref="OtelTapInitializationException"></exception>
        public static OtelTapHttpProtobufReceiver Start(OtelTapHttpProtobufReceiverSettings settings)
        {
            var status = OtelTapCore.oteltap_start_receiving_http_protobuf(
                (ushort)settings.HttpPort,
                (settings.PrintTracesAsNdjson ? OtelTapFlags.PrintTracesAsNdjson : OtelTapFlags.None) |
                (settings.PrintLogsAsNdjson ? OtelTapFlags.PrintLogsAsNdjson : OtelTapFlags.None) |
                (settings.PrintMetricsAsNdjson ? OtelTapFlags.PrintMetricsAsNdjson : OtelTapFlags.None),
                settings.ReemitTracesToUrl,
                settings.ReemitLogsToUrl,
                settings.ReemitMetricsToUrl,
                out ulong handle
            );

            if (status < 0)
            {
                throw new OtelTapInitializationException(
                    $"oteltap_start_receiving_http_protobuf() failed. Error code: {status}",
                    status);
            }
            
            return new OtelTapHttpProtobufReceiver(handle, settings.Log);
        }

        /// <summary>
        /// Starts a <see cref="OtelTapHttpProtobufReceiver"/>.
        /// </summary>
        /// <param name="settings">Config settings</param>
        /// <returns>The running <see cref="OtelTapHttpProtobufReceiver"/> instance.</returns>
        /// <exception cref="OtelTapInitializationException"></exception>
        public static async Task<OtelTapHttpProtobufReceiver> StartAsync(OtelTapHttpProtobufReceiverSettings settings)
        {
            // oteltap_core is all synchronous.
            // Providing this StartAsync() method only for convenience,
            // but also ensuring it does not block the calling thread.
            await Task.Yield();

            return Start(settings);
        }

        /// <summary>
        /// Enumerates through all received traces
        /// </summary>
        public async IAsyncEnumerable<Span> StreamTraces([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var channel = Channel.CreateUnbounded<Span>(
                new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = true
                });

            Subscribe(ref this.traceSubscribers, channel);
            try
            {
                await foreach (var span in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return span;
                }
            }
            finally
            {
                Unsubscribe(ref this.traceSubscribers, channel);
            }
        }

        /// <summary>
        /// Enumerates through all received logs
        /// </summary>
        public async IAsyncEnumerable<LogRecord> StreamLogs([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var channel = Channel.CreateUnbounded<LogRecord>(
                new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = true
                });

            Subscribe(ref this.logSubscribers, channel);
            try
            {
                await foreach (var log in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return log;
                }
            }
            finally
            {
                Unsubscribe(ref this.logSubscribers, channel);
            }
        }

        /// <summary>
        /// Enumerates through all received metrics
        /// </summary>
        public async IAsyncEnumerable<Metric> StreamMetrics([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var channel = Channel.CreateUnbounded<Metric>(
                new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = true
                });

            Subscribe(ref this.metricSubscribers, channel);
            try
            {
                await foreach (var log in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    yield return log;
                }
            }
            finally
            {
                Unsubscribe(ref this.metricSubscribers, channel);
            }
        }

        /// <summary>
        /// Awaits a trace that matches the given predicate.
        /// </summary>
        /// <param name="predicate">Predicate to be matched against incoming traces</param>
        /// <param name="cancellationToken">Cancellation token to cancel the await operation</param>
        /// <returns>Task that completes with the matching trace span</returns>
        public Task<Span> AwaitTraceAsync(Func<Span, bool> predicate, CancellationToken cancellationToken = default)
        {
            var completionSource = new TaskCompletionSource<Span>(TaskCreationOptions.RunContinuationsAsynchronously);

            lock (this.traceAwaitersLock)
            {
                this.traceAwaiters.Add((predicate, completionSource));
            }

            cancellationToken.Register(() =>
            {
                lock (this.traceAwaitersLock)
                {
                    this.traceAwaiters.RemoveAll(a => a.completionSource == completionSource);
                }
                completionSource.TrySetCanceled(cancellationToken);
            });

            return completionSource.Task;
        }

        /// <summary>
        /// Awaits a log that matches the given predicate.
        /// </summary>
        /// <param name="predicate">Predicate to be matched against incoming logs</param>
        /// <param name="cancellationToken">Cancellation token to cancel the await operation</param>
        /// <returns>Task that completes with the matching log record</returns>
        public Task<LogRecord> AwaitLogAsync(Func<LogRecord, bool> predicate, CancellationToken cancellationToken = default)
        {
            var completionSource = new TaskCompletionSource<LogRecord>(TaskCreationOptions.RunContinuationsAsynchronously);

            lock (this.logAwaitersLock)
            {
                this.logAwaiters.Add((predicate, completionSource));
            }

            cancellationToken.Register(() =>
            {
                lock (this.logAwaitersLock)
                {
                    this.logAwaiters.RemoveAll(a => a.completionSource == completionSource);
                }
                completionSource.TrySetCanceled(cancellationToken);
            });

            return completionSource.Task;
        }

        /// <summary>
        /// Awaits a metric that matches the given predicate.
        /// </summary>
        /// <param name="predicate">Predicate to be matched against incoming metrics</param>
        /// <param name="cancellationToken">Cancellation token to cancel the await operation</param>
        /// <returns>Task that completes with the matching metric</returns>
        public Task<Metric> AwaitMetricAsync(Func<Metric, bool> predicate, CancellationToken cancellationToken = default)
        {
            var completionSource = new TaskCompletionSource<Metric>(TaskCreationOptions.RunContinuationsAsynchronously);

            lock (this.metricAwaitersLock)
            {
                this.metricAwaiters.Add((predicate, completionSource));
            }

            cancellationToken.Register(() =>
            {
                lock (this.metricAwaitersLock)
                {
                    this.metricAwaiters.RemoveAll(a => a.completionSource == completionSource);
                }
                completionSource.TrySetCanceled(cancellationToken);
            });

            return completionSource.Task;
        }

        public void Dispose()
        {
            if (this.handle == null)
            {
                return;
            }

            // Cancelling and joining the polling tasks
            this.pollLoopCts.Cancel();

            try
            {
                this.tracesPollingTask.Wait(PollIntervalInMs * 3);
            }
            catch (Exception ex)
            {
                this.log("Exception while stopping the traces polling task", ex);
            }

            try
            {
                this.logsPollingTask.Wait(PollIntervalInMs * 3);
            }
            catch (Exception ex)
            {
                this.log("Exception while stopping the logs polling task", ex);
            }

            try
            {
                this.metricsPollingTask.Wait(PollIntervalInMs * 3);
            }
            catch (Exception ex)
            {
                this.log("Exception while stopping the metrics polling task", ex);
            }

            // Completing all subscribers
            foreach (var channel in this.traceSubscribers)
            {
                channel.Writer.TryComplete();
            }

            foreach (var channel in this.logSubscribers)
            {
                channel.Writer.TryComplete();
            }

            foreach (var channel in this.metricSubscribers)
            {
                channel.Writer.TryComplete();
            }

            // Now stopping the receiver
            try
            {
                int status = OtelTapCore.oteltap_stop_receiving(this.handle.Value);

                if (status < 0)
                {
                    this.log($"oteltap_stop_receiving() failed. Error code: {status}", null);
                }

                this.handle = null;
            }
            catch (Exception ex)
            {
                this.log($"Exception while calling oteltap_stop_receiving().", ex);
            }
        }

        private unsafe void PollTraces(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (this.handle == null)
                {
                    var exception = new OtelTapPollingException($"OtelTapHttpProtobufReceiver is not running", 0);
                    this.log(exception.Message, exception);
                    break;
                }

                int status = OtelTapCore.oteltap_poll_trace(this.handle.Value, PollIntervalInMs, out nint buf, out nuint len);
                if (status < 0)
                {
                    var exception = new OtelTapPollingException($"oteltap_poll_trace() failed. Error code: {status}", status);
                    this.log(exception.Message, exception);
                    break;
                }

                if (len == 0)
                {
                    continue; // timeout, nothing received yet
                }

                // To save time, parsing directly from raw bytes. This is unsafe - hence AllowUnsafeBlocks=true
                var span = Span.Parser.ParseFrom(new ReadOnlySpan<byte>((void*)buf, (int)len));

                // Notifying all subscribers
                foreach (var channel in this.traceSubscribers)
                {
                    channel.Writer.TryWrite(span);
                }

                // Notifying all awaiters
                lock (this.traceAwaitersLock)
                {
                    for (int i = 0; i < this.traceAwaiters.Count; i++)
                    {
                        var (predicate, completionSource) = this.traceAwaiters[i];

                        // If there's an awaiter for this span, complete it and remove it from the list.
                        // Intentionally doing it only once - multiple awaiters must complete independently, and in order.
                        if (predicate(span))
                        {
                            completionSource.TrySetResult(span);
                            this.traceAwaiters.RemoveAt(i);
                            break;
                        }
                    }
                }
            }           
        }

        private unsafe void PollLogs(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (this.handle == null)
                {
                    var exception = new OtelTapPollingException($"OtelTapHttpProtobufReceiver is not running", 0);
                    this.log(exception.Message, exception);
                    break;
                }

                int status = OtelTapCore.oteltap_poll_log(this.handle.Value, PollIntervalInMs, out nint buf, out nuint len);
                if (status < 0)
                {
                    var exception = new OtelTapPollingException($"oteltap_poll_log() failed. Error code: {status}", status);
                    this.log(exception.Message, exception);
                    break;
                }

                if (len == 0)
                {
                    continue; // timeout, nothing received yet
                }

                // To save time, parsing directly from raw bytes. This is unsafe - hence AllowUnsafeBlocks=true
                var log = LogRecord.Parser.ParseFrom(new ReadOnlySpan<byte>((void*)buf, (int)len));

                // Notifying all subscribers
                foreach (var channel in this.logSubscribers)
                {
                    channel.Writer.TryWrite(log);
                }

                // Notifying all awaiters
                lock (this.logAwaitersLock)
                {
                    for (int i = 0; i < this.logAwaiters.Count; i++)
                    {
                        var (predicate, completionSource) = this.logAwaiters[i];

                        // If there's an awaiter for this log, complete it and remove it from the list.
                        // Intentionally doing it only once - multiple awaiters must complete independently, and in order.
                        if (predicate(log))
                        {
                            completionSource.TrySetResult(log);
                            this.logAwaiters.RemoveAt(i);
                            break;
                        }
                    }
                }
            }           
        }

        private unsafe void PollMetrics(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (this.handle == null)
                {
                    var exception = new OtelTapPollingException($"OtelTapHttpProtobufReceiver is not running", 0);
                    this.log(exception.Message, exception);
                    break;
                }

                int status = OtelTapCore.oteltap_poll_metric(this.handle.Value, PollIntervalInMs, out nint buf, out nuint len);
                if (status < 0)
                {
                    var exception = new OtelTapPollingException($"oteltap_poll_metric() failed. Error code: {status}", status);
                    this.log(exception.Message, exception);
                    break;
                }

                if (len == 0)
                {
                    continue; // timeout, nothing received yet
                }

                // To save time, parsing directly from raw bytes. This is unsafe - hence AllowUnsafeBlocks=true
                var metric = Metric.Parser.ParseFrom(new ReadOnlySpan<byte>((void*)buf, (int)len));

                // Notifying all subscribers
                foreach (var channel in this.metricSubscribers)
                {
                    channel.Writer.TryWrite(metric);
                }

                // Notifying all awaiters
                lock (this.metricAwaitersLock)
                {
                    for (int i = 0; i < this.metricAwaiters.Count; i++)
                    {
                        var (predicate, completionSource) = this.metricAwaiters[i];

                        // If there's an awaiter for this metric, complete it and remove it from the list.
                        // Intentionally doing it only once - multiple awaiters must complete independently, and in order.
                        if (predicate(metric))
                        {
                            completionSource.TrySetResult(metric);
                            this.metricAwaiters.RemoveAt(i);
                            break;
                        }
                    }
                }
            }           
        }

        private void Subscribe<T>(ref ImmutableArray<Channel<T>> channels, Channel<T> channel) =>
            ImmutableInterlocked.Update(ref channels, subs => subs.Add(channel));

        private void Unsubscribe<T>(ref ImmutableArray<Channel<T>> channels, Channel<T> channel) =>
            ImmutableInterlocked.Update(ref channels, subs => subs.Remove(channel));
    }
}
