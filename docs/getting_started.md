# Getting Started

This guide walks you through installing TimelessTraces, wiring it into the OpenTelemetry SDK, and running your first trace query.

## Installation

Add the dependency to your `mix.exs`:

```elixir
def deps do
  [
    {:timeless_traces, "~> 0.5"},
    {:opentelemetry_api, "~> 1.4"},
    {:opentelemetry, "~> 1.5"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

## Configuration

Set a data directory and wire up the OTel exporter:

```elixir
# config/config.exs
config :timeless_traces,
  data_dir: "priv/span_stream"

config :opentelemetry,
  traces_exporter: {TimelessTraces.Exporter, []}
```

That's it. TimelessTraces starts automatically as an OTP application. The exporter receives spans directly from the OpenTelemetry Erlang SDK -- no HTTP, no protobuf, no external collectors.

## Creating spans

Use the standard OpenTelemetry API. Spans are automatically exported to TimelessTraces:

```elixir
require OpenTelemetry.Tracer, as: Tracer

Tracer.with_span "my_operation" do
  # Your code here
  Tracer.set_attribute("user.id", "abc123")
  :ok
end
```

For Phoenix applications, add `opentelemetry_phoenix` and `opentelemetry_ecto` for automatic instrumentation:

```elixir
def deps do
  [
    {:opentelemetry_phoenix, "~> 1.2"},
    {:opentelemetry_ecto, "~> 1.2"}
  ]
end
```

```elixir
# In your application.ex start/2
OpentelemetryPhoenix.setup()
OpentelemetryEcto.setup([:my_app, :repo])
```

## Querying spans

Once spans are flowing, query them:

```elixir
# Find recent error spans
{:ok, result} = TimelessTraces.query(status: :error)
result.entries  # => [%TimelessTraces.Span{}, ...]
result.total    # => 42

# Find slow spans (> 100ms)
TimelessTraces.query(min_duration: 100_000_000)

# Find spans from a specific service
TimelessTraces.query(service: "my_app", kind: :server)
```

## Trace lookup

Retrieve all spans in a single trace:

```elixir
{:ok, spans} = TimelessTraces.trace("abc123def456...")
# Returns all spans sorted by start time
```

## Listing services and operations

```elixir
{:ok, services} = TimelessTraces.services()
# => {:ok, ["my_app", "api_gateway", "auth_service"]}

{:ok, ops} = TimelessTraces.operations("my_app")
# => {:ok, ["GET /users", "DB query", "cache_lookup"]}
```

## Statistics

Check storage status:

```elixir
{:ok, stats} = TimelessTraces.stats()
stats.total_blocks   #=> 42
stats.total_entries   #=> 50_000
stats.disk_size       #=> 24_000_000
```

## Enabling the HTTP API

Optionally enable the HTTP interface for Jaeger-compatible queries and OTLP ingest:

```elixir
config :timeless_traces, http: true                          # port 10428, no auth
config :timeless_traces, http: [port: 10500, bearer_token: "secret"]
```

See the [HTTP API](http_api.md) guide for endpoint details.

## Next steps

- [Configuration Reference](configuration.md) -- all options with defaults
- [Architecture](architecture.md) -- how the pipeline works
- [Querying](querying.md) -- full filter reference and optimization tips
- [HTTP API](http_api.md) -- OTLP ingest and Jaeger-compatible query endpoints
- [Storage & Compression](storage.md) -- block formats and compaction
- [Operations](operations.md) -- backup, retention, and troubleshooting
