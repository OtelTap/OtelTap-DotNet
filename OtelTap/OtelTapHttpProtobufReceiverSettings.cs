
namespace OtelTap
{
    /// <summary>
    /// Settings for OtelTapHttpProtobufReceiver
    /// </summary>
    /// <param name="HttpPort">HTTP port number to listen on</param>
    /// <param name="ListenOnAllInterfaces">Should the receiver listen on all network interfaces</param>
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
        bool ListenOnAllInterfaces,
        bool PrintTracesAsNdjson,
        bool PrintLogsAsNdjson,
        bool PrintMetricsAsNdjson,
        string? ReemitTracesToUrl,
        string? ReemitLogsToUrl,
        string? ReemitMetricsToUrl,
        Action<string, Exception?>? Log
    )
    {
    }
}
