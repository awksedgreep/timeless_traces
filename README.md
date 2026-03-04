# TimelessTraces

> "I always found it odd that the first thing you do to time series data is squash the timestamp. That's how the name Timeless was born." --Mark Cotner

Embedded OpenTelemetry span storage and compression for Elixir applications.

TimelessTraces receives spans directly from the OpenTelemetry Erlang SDK (no HTTP, no protobuf), compresses them with two-tier raw/OpenZL block storage (~10x compression), and indexes them in ETS for lock-free trace-level and span-level queries. Zero external infrastructure required.

Part of the embedded observability stack:
- [timeless_metrics](https://github.com/awksedgreep/timeless_metrics) - numeric time series compression
- [timeless_logs](https://github.com/awksedgreep/timeless_logs) - log ingestion/compression/indexing
- **timeless_traces** - OTel span storage/compression (this library)

## Documentation

- [Getting Started](docs/getting_started.md)
- [Configuration Reference](docs/configuration.md)
- [Architecture](docs/architecture.md)
- [Querying](docs/querying.md)
- [HTTP API](docs/http_api.md)
- [Live Tail (Subscriptions)](docs/subscriptions.md)
- [Storage & Compression](docs/storage.md)
- [Operations](docs/operations.md)
- [Telemetry](docs/telemetry.md)

## Installation

```elixir
def deps do
  [
    {:timeless_traces, "~> 1.0"}
  ]
end
```

## Configuration

```elixir
# config/config.exs
config :timeless_traces,
  storage: :disk,              # :disk or :memory
  data_dir: "priv/span_stream",
  flush_interval: 1_000,       # ms between auto-flushes
  max_buffer_size: 1_000,      # spans before forced flush
  compaction_threshold: 500,   # raw entries before compaction
  compaction_format: :openzl,  # :openzl (default) or :zstd
  compression_level: 6,        # compression level 1-22 (default 6)
  retention_max_age: nil,      # seconds, nil = no age limit
  retention_max_size: nil      # bytes, nil = no size limit

# Wire up the OTel exporter
config :opentelemetry,
  traces_exporter: {TimelessTraces.Exporter, []}
```

## Usage

### Querying spans

```elixir
# All error spans
TimelessTraces.query(status: :error)

# Server spans from a specific service
TimelessTraces.query(kind: :server, service: "api-gateway")

# Slow spans (> 100ms)
TimelessTraces.query(min_duration: 100_000_000)

# Combined filters with pagination
TimelessTraces.query(status: :error, kind: :server, limit: 50, order: :desc)
```

### Trace lookup

```elixir
# Get all spans in a trace, sorted by start time
{:ok, spans} = TimelessTraces.trace("abc123def456...")
```

### Live tail

```elixir
# Subscribe to new spans as they arrive
TimelessTraces.subscribe(status: :error)

receive do
  {:timeless_traces, :span, %TimelessTraces.Span{} = span} ->
    IO.inspect(span.name)
end
```

### Statistics

```elixir
{:ok, stats} = TimelessTraces.stats()
stats.total_blocks   #=> 42
stats.total_entries   #=> 50_000
stats.disk_size       #=> 24_000_000
```

## Query Filters

| Filter | Type | Description |
|---|---|---|
| `:name` | string | Substring match on span name |
| `:service` | string | Match `service.name` in attributes or resource |
| `:kind` | atom | `:internal`, `:server`, `:client`, `:producer`, `:consumer` |
| `:status` | atom | `:ok`, `:error`, `:unset` |
| `:min_duration` | integer | Minimum duration in nanoseconds |
| `:max_duration` | integer | Maximum duration in nanoseconds |
| `:since` | integer/DateTime | Start time lower bound (nanos or DateTime) |
| `:until` | integer/DateTime | Start time upper bound (nanos or DateTime) |
| `:trace_id` | string | Filter to specific trace |
| `:attributes` | map | Key/value pairs to match |
| `:limit` | integer | Max results (default 100) |
| `:offset` | integer | Skip N results (default 0) |
| `:order` | atom | `:desc` (default) or `:asc` |

## Architecture

```
OTel SDK → Exporter → Buffer → Writer (raw) → ETS Index → disk log
                                    ↓
                              Compactor (OpenZL/zstd)
```

- **Buffer** accumulates spans, flushes every 1s or 1000 spans
- **Writer** serializes blocks as raw Erlang terms initially
- **Index** stores block metadata + inverted term index + trace index in ETS, persisted via snapshots + disk log
- **Compactor** merges raw blocks into compressed blocks (zstd or OpenZL columnar)
- **Retention** enforces age and size limits

### Storage Modes

- **`:disk`** - Blocks as files in `data_dir/blocks/`, index persisted as `index.snapshot` + `index.log`
- **`:memory`** - Blocks in ETS tables only, no filesystem needed

## Compression

Two compression backends are supported. OpenZL columnar compression (default) achieves better ratios and faster queries by encoding span fields in typed columns:

| Backend | Size (500K spans) | Ratio | Compress | Decompress |
|---|---|---|---|---|
| zstd | 32.8 MB | 6.8x | 2.1s | 1.5s |
| OpenZL columnar | 22.1 MB | 10.2x | 1.9s | 564ms |

## Performance

Run on a 28-core laptop. Reproduce with `mix timeless_traces.ingest_benchmark`, `mix timeless_traces.compression_benchmark`, and `mix timeless_traces.search_benchmark`.

Ingestion throughput on 500K spans (1000 spans/block):

| Phase | Throughput |
|---|---|
| Writer only (serialization + disk I/O) | ~196K spans/sec |
| Writer + Index (ETS immediate + disk log persist) | ~468K spans/sec |
| Full pipeline (Buffer → Writer → async Index) | ~303K spans/sec |

The ETS-first indexing architecture makes index overhead negligible.

Query latency (500K spans, 500 blocks, avg over 3 runs):

| Query | zstd | OpenZL | Speedup |
|---|---|---|---|
| All spans (limit 100) | 1.12s | 860ms | 1.3x |
| status=error | 327ms | 158ms | 2.1x |
| service filter | 316ms | 225ms | 1.4x |
| kind=server | 324ms | 226ms | 1.4x |
| Trace lookup | 7.1ms | 10.0ms | 0.7x |

## License

MIT - see [LICENSE](LICENSE)
