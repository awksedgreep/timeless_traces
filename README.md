# SpanStream

Embedded OpenTelemetry span storage and compression for Elixir applications.

SpanStream receives spans directly from the OpenTelemetry Erlang SDK (no HTTP, no protobuf), compresses them with two-tier raw/zstd block storage (~8.8x compression), and indexes them in SQLite for fast trace-level and span-level queries. Zero external infrastructure required.

Part of the embedded observability stack:
- [gorilla_stream](https://github.com/awksedgreep/gorilla_stream) - numeric time series compression
- [log_stream](https://github.com/awksedgreep/log_stream) - log ingestion/compression/indexing
- **span_stream** - OTel span storage/compression (this library)

## Installation

```elixir
def deps do
  [
    {:span_stream, "~> 0.1.0"}
  ]
end
```

## Configuration

```elixir
# config/config.exs
config :span_stream,
  storage: :disk,              # :disk or :memory
  data_dir: "priv/span_stream",
  flush_interval: 1_000,       # ms between auto-flushes
  max_buffer_size: 1_000,      # spans before forced flush
  compaction_threshold: 500,   # raw entries before zstd compaction
  retention_max_age: nil,      # seconds, nil = no age limit
  retention_max_size: nil      # bytes, nil = no size limit

# Wire up the OTel exporter
config :opentelemetry,
  traces_exporter: {SpanStream.Exporter, []}
```

## Usage

### Querying spans

```elixir
# All error spans
SpanStream.query(status: :error)

# Server spans from a specific service
SpanStream.query(kind: :server, service: "api-gateway")

# Slow spans (> 100ms)
SpanStream.query(min_duration: 100_000_000)

# Combined filters with pagination
SpanStream.query(status: :error, kind: :server, limit: 50, order: :desc)
```

### Trace lookup

```elixir
# Get all spans in a trace, sorted by start time
{:ok, spans} = SpanStream.trace("abc123def456...")
```

### Live tail

```elixir
# Subscribe to new spans as they arrive
SpanStream.subscribe(status: :error)

receive do
  {:span_stream, :span, %SpanStream.Span{} = span} ->
    IO.inspect(span.name)
end
```

### Statistics

```elixir
{:ok, stats} = SpanStream.stats()
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
- **Compactor** merges raw blocks into zstd-compressed blocks (~8.8x ratio at 500 spans)
- **Retention** enforces age and size limits

### Storage Modes

- **`:disk`** - Blocks as files in `data_dir/blocks/`, index in `data_dir/index.db`
- **`:memory`** - Blocks as BLOBs in SQLite `:memory:`, no filesystem needed

## Compression

Spans compress well with zstd. Sweet spot is 200-500 spans per block:

| Spans | Raw | Compressed | Ratio |
|---|---|---|---|
| 1 | 670 B | 470 B | 1.4x |
| 10 | 6.0 KB | 1.2 KB | 4.9x |
| 100 | 60.1 KB | 7.4 KB | 8.1x |
| 500 | 301.6 KB | 34.4 KB | 8.8x |
| 5000 | 3.0 MB | 338.0 KB | 8.9x |

## License

MIT - see [LICENSE](LICENSE)
