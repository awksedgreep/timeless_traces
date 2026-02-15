# TimelessTraces

Embedded OpenTelemetry span storage and compression for Elixir applications.

TimelessTraces receives spans directly from the OpenTelemetry Erlang SDK (no HTTP, no protobuf), compresses them with two-tier raw/zstd block storage (~9.9x compression), and indexes them in SQLite for fast trace-level and span-level queries. Zero external infrastructure required.

Part of the embedded observability stack:
- [timeless_metrics](https://github.com/awksedgreep/timeless_metrics) - numeric time series compression
- [timeless_logs](https://github.com/awksedgreep/timeless_logs) - log ingestion/compression/indexing
- **timeless_traces** - OTel span storage/compression (this library)

## Installation

```elixir
def deps do
  [
    {:timeless_traces, "~> 0.2.0"}
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
  compaction_threshold: 500,   # raw entries before zstd compaction
  compression_level: 6,        # zstd level 1-22 (default 6)
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
OTel SDK → Exporter → Buffer → Writer (raw) → SQLite Index
                                    ↓
                              Compactor (zstd)
```

- **Buffer** accumulates spans, flushes every 1s or 1000 spans
- **Writer** serializes blocks as raw Erlang terms initially
- **Index** stores block metadata + inverted term index + trace index in SQLite
- **Compactor** merges raw blocks into zstd-compressed blocks (~9.9x ratio at 500 spans)
- **Retention** enforces age and size limits

### Storage Modes

- **`:disk`** - Blocks as files in `data_dir/blocks/`, index in `data_dir/index.db`
- **`:memory`** - Blocks as BLOBs in SQLite `:memory:`, no filesystem needed

## Compression

Spans compress well with zstd (level 6). Sweet spot is 200-500 spans per block:

| Spans | Raw | Compressed | Ratio |
|---|---|---|---|
| 1 | 610 B | 421 B | 1.4x |
| 10 | 6.0 KB | 1.2 KB | 5.2x |
| 100 | 60.3 KB | 6.6 KB | 9.1x |
| 500 | 302.1 KB | 30.4 KB | 9.9x |
| 5000 | 3.0 MB | 297.1 KB | 10.2x |

## Performance

Ingestion throughput on 500K spans (1000 spans/block):

| Phase | Throughput |
|---|---|
| Writer only (serialization + disk I/O) | ~260K spans/sec |
| Writer + Index (sync SQLite indexing) | ~19K spans/sec |
| Full pipeline (Buffer → Writer → async Index) | ~66K spans/sec |

## License

MIT - see [LICENSE](LICENSE)
