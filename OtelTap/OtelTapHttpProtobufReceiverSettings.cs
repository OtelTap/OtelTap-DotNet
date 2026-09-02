
namespace OtelTap
{
    /// <summary>
    /// Settings for OtelTapHttpProtobufReceiver
    /// </summary>
    /// <param name="HttpPort">HTTP port number to listen on</param>
    /// <param name="ListenOnAllInterfaces">Should the receiver listen on all network interfaces. When false (default), listens only on loopback interface (127.0.0.1).</param>
    /// <param name="PrintTracesAsNdjson">Should traces be printed into standard output (as NDJSON)</param>
    /// <param name="PrintLogsAsNdjson">Should logs be printed into standard output (as NDJSON)</param>
    /// <param name="PrintMetricsAsNdjson">Should logs be printed into standard output (as NDJSON)</param>
    /// <param name="ReemitTracesToUrl">URL to re-emit traces to, e.g. http://localhost:4318/v1/traces</param>
    /// <param name="ReemitLogsToUrl">URL to re-emit logs to, e.g. http://localhost:4318/v1/logs</param>
    /// <param name="ReemitMetricsToUrl">URL to re-emit metrics to, e.g. http://localhost:4318/v1/metrics</param>
    /// <param name="Log">Optional logging routine</param>
    public record OtelTapHttpProtobufReceiverSettings
    (
        int HttpPort,
        bool ListenOnAllInterfaces = false,
        bool PrintTracesAsNdjson = false,
        bool PrintLogsAsNdjson = false,
        bool PrintMetricsAsNdjson = false,
        string? ReemitTracesToUrl = null,
        string? ReemitLogsToUrl = null,
        string? ReemitMetricsToUrl = null,
        Action<string, Exception?>? Log = null
    )
    {
    }
}
