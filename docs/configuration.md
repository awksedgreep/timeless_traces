# Configuration Reference

All configuration is set under the `:timeless_traces` application key in `config.exs` or at runtime via `Application.put_env/3`.

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | string | `"priv/span_stream"` | Root directory for blocks and index |
| `storage` | atom | `:disk` | Storage backend (`:disk` or `:memory`) |
| `flush_interval` | integer (ms) | `1_000` | Buffer auto-flush interval |
| `max_buffer_size` | integer | `1_000` | Max spans before forced flush |
| `query_timeout` | integer (ms) | `30_000` | Query timeout |
| `compaction_threshold` | integer | `500` | Min raw entries to trigger compaction |
| `compaction_interval` | integer (ms) | `30_000` | Compaction check interval |
| `compaction_max_raw_age` | integer (s) | `60` | Force compact raw blocks older than this |
| `compaction_format` | atom | `:openzl` | Compression format (`:openzl` or `:zstd`) |
| `compression_level` | integer | `6` | Compression level (1-22) |
| `index_publish_interval` | integer (ms) | `2_000` | Index batch write interval |
| `retention_max_age` | integer (s) or nil | `604_800` (7 days) | Max span age (`nil` = keep forever) |
| `retention_max_size` | integer (bytes) or nil | `536_870_912` (512 MB) | Max storage size (`nil` = unlimited) |
| `retention_check_interval` | integer (ms) | `300_000` (5 min) | Retention check interval |
| `http` | boolean or keyword | `false` | Enable HTTP API |

## HTTP options

When `http` is a keyword list:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | integer | `10428` | HTTP listen port |
| `bearer_token` | string or nil | `nil` | Bearer token for authentication |

## Full example

```elixir
# config/config.exs
config :timeless_traces,
  # Storage
  storage: :disk,
  data_dir: "priv/span_stream",

  # Buffer
  flush_interval: 1_000,
  max_buffer_size: 1_000,

  # Compaction
  compaction_threshold: 500,
  compaction_interval: 30_000,
  compaction_max_raw_age: 60,
  compaction_format: :openzl,
  compression_level: 6,

  # Indexing
  index_publish_interval: 2_000,

  # Query
  query_timeout: 30_000,

  # Retention
  retention_max_age: 7 * 86_400,
  retention_max_size: 512 * 1_048_576,
  retention_check_interval: 300_000,

  # HTTP API
  http: [port: 10428, bearer_token: "my-secret-token"]

# OTel exporter
config :opentelemetry,
  traces_exporter: {TimelessTraces.Exporter, []}
```

## OpenTelemetry exporter configuration

Wire TimelessTraces as the OTel traces exporter:

```elixir
config :opentelemetry,
  traces_exporter: {TimelessTraces.Exporter, []}
```

The exporter reads spans directly from the OTel SDK's ETS table -- no HTTP or protobuf involved. This is significantly lower overhead than sending spans to an external collector.

## Storage backends

### Disk (default)

Blocks are stored as files in `data_dir/blocks/` and the index in `data_dir/index.db` (SQLite WAL mode):

```elixir
config :timeless_traces, storage: :disk
```

### Memory

Blocks are stored as BLOBs in an in-memory SQLite database. No files are written to disk. Data does not survive restarts. Useful for testing:

```elixir
config :timeless_traces, storage: :memory
```

## Tuning guidance

### Buffer size and flush interval

The defaults (1000 spans / 1 second) balance latency and throughput. For high-volume services:

- Increase `max_buffer_size` to 5000-10000 for higher throughput
- Increase `flush_interval` to 5000ms to reduce disk I/O
- Decrease both for lower query latency (spans become queryable after flush)

### Compression

The default format is `:openzl` (columnar compression), which achieves ~10x compression on span data with faster query-time decompression than zstd.

| Format | Compression ratio | Best for |
|--------|-------------------|----------|
| `:openzl` | ~10.0x | General use (default), fastest queries |
| `:zstd` | ~6.8x | Simpler, fast compression |

The `compression_level` setting applies to both formats (1-22, higher = smaller but slower).

### Retention

Both age-based and size-based retention are enabled by default. Set either to `nil` to disable:

```elixir
config :timeless_traces,
  retention_max_age: nil,    # No age limit
  retention_max_size: nil    # No size limit
```

### Index publish interval

The index batches SQLite writes every 2 seconds by default. Lower values mean spans become queryable faster but increase SQLite write load. For most workloads the default is fine.
