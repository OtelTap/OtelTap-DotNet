using System.Reflection;
using System.Runtime.InteropServices;

namespace OtelTap
{
    // Bitwise flags for oteltap_start_receiving_http_protobuf, mirroring ffi_functions_flags.rs.
    [Flags]
    internal enum OtelTapFlags : uint
    {
        None = 0,
        PrintTracesAsNdjson = 1 << 0,
        PrintLogsAsNdjson = 1 << 1,
        PrintMetricsAsNdjson = 1 << 2,

        ListenOnAllInterfaces = 1 << 8,
    }

    /// <summary>
    /// P/Invoke wrapper around the native oteltap_core lib.
    /// </summary>
    internal static partial class OtelTapCore
    {
        private const string OtelTapLibraryFileName = "oteltap_core";

        static OtelTapCore()
        {
            // Dynamically loading the native lib in a cross-platform way
            NativeLibrary.SetDllImportResolver(typeof(OtelTapCore).Assembly, ResolveLibrary);
        }

        /// <summary>
        /// Cross-platform resolver.
        /// </summary>
        private static nint ResolveLibrary(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (libraryName != OtelTapLibraryFileName)
            {
                return nint.Zero;
            }

            string fileName =
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "oteltap_core.dll" :
                RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "liboteltap_core.dylib" :
                "liboteltap_core.so";

            return NativeLibrary.Load(fileName, assembly, searchPath);
        }

        /// <summary>
        /// Starts OtelTap receiver on the specified port, expecting http/protobuf format, with optional re-emission endpoints for traces, logs, and metrics.
        /// </summary>
        [LibraryImport(OtelTapLibraryFileName, StringMarshalling = StringMarshalling.Utf8)]
        public static partial int oteltap_start_receiving_http_protobuf(
            ushort port,
            OtelTapFlags flags,
            string? reemitTracesTo,
            string? reemitLogsTo,
            string? reemitMetricsTo,
            out ulong outHandle);

        /// <summary>
        /// Stops the OtelTap receiver associated with the given handle, cleaning up resources.
        /// </summary>
        [LibraryImport(OtelTapLibraryFileName)]
        public static partial int oteltap_stop_receiving(ulong handle);

        /// <summary>
        /// Polls for a trace span. On success, out_buf/out_len describe a protobuf-encoded Span
        /// </summary>
        [LibraryImport(OtelTapLibraryFileName)]
        public static partial int oteltap_poll_trace(ulong handle, ulong timeoutMs, out nint outBuf, out nuint outLen);

        /// <summary>
        /// Polls for a log record. Same buffer ownership rules as oteltap_poll_trace.
        /// </summary>
        [LibraryImport(OtelTapLibraryFileName)]
        public static partial int oteltap_poll_log(ulong handle, ulong timeoutMs, out nint outBuf, out nuint outLen);

        /// <summary>
        /// Polls for a metric. Same buffer ownership rules as oteltap_poll_trace.
        /// </summary>
        [LibraryImport(OtelTapLibraryFileName)]
        public static partial int oteltap_poll_metric(ulong handle, ulong timeoutMs, out nint outBuf, out nuint outLen);
    }
}
