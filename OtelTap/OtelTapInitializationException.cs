
namespace OtelTap
{
    /// <summary>
    /// Thrown if OtelTap fails to initialize
    /// </summary>
    public class OtelTapInitializationException : Exception
    {
        /// <summary>
        /// Error code returned by oteltap_core
        /// </summary>
        public int ErrorCode { get; set; }

        public OtelTapInitializationException(string message, int errorCode) : base(message)
        {
            this.ErrorCode = errorCode;
        }
    }
}
