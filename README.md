# OtelTap-DotNet

**OtelTap** is a .NET (C#) client library that wraps [OtelTap-Rust](https://github.com/OtelTap/OtelTap-Rust)'s embeddable OTLP (OpenTelemetry Protocol) receiver, giving .NET tests an idiomatic, `async`/`IAsyncEnumerable`-based API to **await and assert on real telemetry** (traces, logs, metrics) emitted by the system under test — instead of guessing timing or mocking the OTel SDK.

[![.NET](https://github.com/OtelTap/OtelTap-DotNet/actions/workflows/dotnet.yml/badge.svg)](https://github.com/OtelTap/OtelTap-DotNet/actions/workflows/dotnet.yml)
[![NuGet](https://img.shields.io/nuget/v/OtelTap.svg)](https://www.nuget.org/packages/OtelTap)

## Built for agentic AI development

OtelTap is designed with **AI coding agents ("copilots") as first-class users**, not just an afterthought. Telemetry is one of the richest sources of ground truth about what a system actually did — far more reliable than logs alone or guessing from source code — and OtelTap is built so an agent can close the loop on its own, without a human relaying data back and forth:

1. **Run** an integration/e2e test that exercises the system under test.
2. **See telemetry instantly** in the console as NDJSON, right in the same tool-call output the agent already reads — no separate viewer, no polling a dashboard, no screenshots.
3. **Infer** what actually happened — which spans fired, what attributes/status/errors show up, what got logged, what metrics moved — directly from that structured output.
4. **Apply code changes** based on that evidence, immediately, and re-run to verify — all within the same agentic loop, with re-emission ensuring humans watching the normal observability stack still see the exact same picture.

## Why

When testing a service that emits OpenTelemetry data, you usually want to:

1. Spin up a lightweight OTLP endpoint the service can point at.
2. Wait for a specific span/log/metric to show up, then assert on its contents.
3. Still let the data flow through to your normal observability stack, so you don't lose visibility while testing.
4. See what's coming through, right in the test/CI console, without attaching a separate viewer.

`OtelTapHttpProtobufReceiver` does all of this from plain C#:

- **Receives** OTLP/HTTP (protobuf) traces, logs, and metrics on a local port.
- Lets you **await** a specific span/log/metric matching a predicate, or **stream** everything as an `IAsyncEnumerable`.
- **Prints incoming telemetry to the console as NDJSON**, one compact JSON object per line — handy for humans, CI logs, and AI coding agents ("copilots") tailing test output.
- **Re-emits** everything it receives to another OTLP/HTTP endpoint, so your usual pipelines/visualizers keep working unmodified while the tap is attached.

## How it works

`OtelTapHttpProtobufReceiver` is a thin, safe C# wrapper around the native `oteltap_core` library (built from [OtelTap-Rust](https://github.com/OtelTap/OtelTap-Rust)), which it calls via `LibraryImport`/P/Invoke (see `OtelTap/OtelTapCore.cs`). Under the hood, `oteltap_core` listens on `127.0.0.1:<port>` for standard OTLP/HTTP protobuf requests (`/v1/traces`, `/v1/logs`, `/v1/metrics`), decodes them, and hands each item back to .NET, which:

- Parses it into the corresponding generated OTLP protobuf message (`Span`, `LogRecord`, `Metric` — see `OtelTap/GeneratedOtlpProtobuf/`).
- Fans it out to any active `StreamTraces`/`StreamLogs`/`StreamMetrics` subscribers and any matching `AwaitTraceAsync`/`AwaitLogAsync`/`AwaitMetricAsync` predicates.
- Optionally prints it to the console as NDJSON and/or re-emits it to another OTLP/HTTP endpoint, both handled natively by `oteltap_core`.

Background polling loops (one per signal type) continuously pull decoded items from the native library and dispatch them; `Dispose()` stops the receiver, cancels the polling loops, and releases the native handle.

> **Note:** only the **OTLP/HTTP protobuf** transport is supported (`Content-Type: application/x-protobuf`) — this is the recommended encoding for OTLP over HTTP (the OTLP spec treats HTTP/JSON as debug-only). OTLP/gRPC is not implemented.

## Installation

OtelTap is published on [NuGet.org](https://www.nuget.org/packages/OtelTap) as the [`OtelTap`](https://www.nuget.org/packages/OtelTap) package, with native `oteltap_core` binaries bundled for `win-x64`, `win-arm64`, `linux-x64`, `linux-arm64`, `osx-x64`, and `osx-arm64` — no separate Rust toolchain or build step required for consumers.

```sh
dotnet add package OtelTap
```

```xml
<ItemGroup>
  <PackageReference Include="OtelTap" Version="1.0.0" />
</ItemGroup>
```

## Usage

```csharp
using OtelTap;

var receiver = OtelTapHttpProtobufReceiver.Start(new OtelTapHttpProtobufReceiverSettings(
    HttpPort: 4318,
    ListenOnAllInterfaces: false,
    PrintTracesAsNdjson: true,
    PrintLogsAsNdjson: true,
    PrintMetricsAsNdjson: true,
    ReemitTracesToUrl: "http://localhost:14318/v1/traces",
    ReemitLogsToUrl: "http://localhost:14318/v1/logs",
    ReemitMetricsToUrl: "http://localhost:14318/v1/metrics",
    Log: (message, ex) => Console.WriteLine($"{message}: {ex}")
));

// ... exercise the system under test, which sends OTLP to http://localhost:4318 ...

// Await a specific span matching a predicate:
var span = await receiver.AwaitTraceAsync(s => s.Name == "checkout", cancellationToken);

// ...or stream everything as it arrives:
await foreach (var log in receiver.StreamLogs(cancellationToken))
{
    Console.WriteLine(log.Body);
}

receiver.Dispose();
```

| Member | Purpose |
|---|---|
| `OtelTapHttpProtobufReceiver.Start(settings)` / `StartAsync(settings)` | Starts a receiver on `settings.HttpPort`. Throws `OtelTapInitializationException` on failure. |
| `StreamTraces(cancellationToken)` / `StreamLogs(...)` / `StreamMetrics(...)` | Returns an `IAsyncEnumerable<T>` yielding every received item of that signal type. |
| `AwaitTraceAsync(predicate, cancellationToken)` / `AwaitLogAsync(...)` / `AwaitMetricAsync(...)` | Returns a `Task<T>` that completes as soon as a received item matches `predicate`. |
| `Dispose()` | Stops the receiver, cancels background polling, and releases the native handle. |

`Span`, `LogRecord`, and `Metric` are the standard `opentelemetry-proto` generated types (see `OtelTap/GeneratedOtlpProtobuf/`), so all the usual fields (attributes, status, resource, etc.) are available directly on the objects you await or stream.

## Prerequisites & building from source

- [.NET SDK](https://dotnet.microsoft.com/) — targets `net10.0`.
- [Rust toolchain](https://rustup.rs/) — edition 2024, so **Rust 1.85 or newer** — required to build the native `oteltap_core` dependency.
- A checkout of **[OtelTap-Rust](https://github.com/OtelTap/OtelTap-Rust) as a sibling directory** to this repo — i.e. both `OtelTap-Rust/` and `OtelTap-DotNet/` under the same parent folder — since `OtelTap.csproj` builds `oteltap-core` from `../../OtelTap-Rust/oteltap-core` (relative to the `OtelTap/` project folder) via `cargo build --release` as a pre-build step.

```
some-parent-folder/
├── OtelTap-Rust/       <-- clone here
└── OtelTap-DotNet/     <-- this repo
```

Build:

```sh
dotnet build OtelTap/OtelTap.csproj -c Release
```

This automatically invokes `cargo build --release` against the sibling `OtelTap-Rust/oteltap-core` checkout and copies the resulting native library (`oteltap_core.dll` / `liboteltap_core.so` / `liboteltap_core.dylib`, depending on OS) alongside the managed assembly.

## Project layout

```
OtelTap-DotNet/
├── OtelTap.sln
└── OtelTap/
    ├── OtelTapCore.cs                          # LibraryImport/P-Invoke surface over the native oteltap_core lib
    ├── OtelTapHttpProtobufReceiver.cs           # Public API: Start/StartAsync, Stream*, Await*, Dispose
    ├── OtelTapHttpProtobufReceiverSettings.cs   # Settings record passed to Start/StartAsync
    ├── OtelTapInitializationException.cs        # Thrown when the native receiver fails to start
    ├── OtelTapPollingException.cs                # Thrown when polling the native receiver fails
    ├── GeneratedOtlpProtobuf/                   # Generated opentelemetry-proto C# types (Trace, Logs, Metrics, Resource, Common)
    └── OtelTap.csproj                           # Builds oteltap-core (Rust) as a pre-build step
```

## Status

This is an early-stage project; the public API and error codes may still change.

## Contributing

Is very much welcomed.
