
namespace OtelTap
{
    /// <summary>
    /// Thrown if OtelTap fails to poll data
    /// </summary>
    public class OtelTapPollingException : Exception
    {
        /// <summary>
        /// Error code returned by oteltap_core
        /// </summary>
        public int ErrorCode { get; set; }

        public OtelTapPollingException(string message, int errorCode) : base(message)
        {
            this.ErrorCode = errorCode;
        }
    }
}
