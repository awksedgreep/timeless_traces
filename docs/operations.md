# Operations

This guide covers monitoring, backup, retention, and troubleshooting for TimelessTraces.

## Statistics

Get aggregate storage statistics without reading any blocks:

```elixir
{:ok, stats} = TimelessTraces.stats()
```

Returns a `%TimelessTraces.Stats{}` struct:

| Field | Description |
|-------|-------------|
| `total_blocks` | Number of stored blocks (raw + compressed) |
| `total_entries` | Total spans across all blocks |
| `total_bytes` | Total block storage size |
| `disk_size` | On-disk storage size |
| `index_size` | SQLite index file size |
| `oldest_timestamp` | Timestamp of oldest span (nanoseconds) |
| `newest_timestamp` | Timestamp of newest span (nanoseconds) |
| `raw_blocks` | Number of uncompressed raw blocks |
| `raw_bytes` | Size of raw blocks |
| `raw_entries` | Entries in raw blocks |
| `zstd_blocks` | Number of zstd-compressed blocks |
| `zstd_bytes` | Size of zstd blocks |
| `zstd_entries` | Entries in zstd blocks |
| `openzl_blocks` | Number of OpenZL-compressed blocks |
| `openzl_bytes` | Size of OpenZL blocks |
| `openzl_entries` | Entries in OpenZL blocks |
| `compression_raw_bytes_in` | Total raw bytes processed by compactor |
| `compression_compressed_bytes_out` | Total compressed bytes produced |
| `compaction_count` | Number of compaction runs |

### HTTP API

```bash
curl http://localhost:10428/health
```

Returns `status`, `blocks`, `spans`, and `disk_size`.

## Flushing

Force flush the buffer to write pending spans to storage immediately:

```elixir
TimelessTraces.flush()
```

```bash
curl http://localhost:10428/api/v1/flush
```

Use before backups or graceful shutdowns.

## Backup

Create a consistent online backup without stopping the application.

### Elixir API

```elixir
{:ok, result} = TimelessTraces.backup("/tmp/span_backup")
# => {:ok, %{path: "/tmp/span_backup", files: ["index.db", "blocks"], total_bytes: 24000000}}
```

### HTTP API

```bash
curl -X POST http://localhost:10428/api/v1/backup \
  -H 'Content-Type: application/json' \
  -d '{"path": "/tmp/span_backup"}'
```

### Backup procedure

1. The buffer is flushed (all pending spans written to storage)
2. SQLite index is snapshot via `VACUUM INTO` (atomic, consistent)
3. Block files are copied in parallel to the target directory
4. Returns the backup path, file list, and total bytes

### Restore procedure

1. Stop the TimelessTraces application
2. Replace the data directory contents with the backup files
3. Start the application -- it will load from the restored data

## Retention

Retention runs automatically to prevent unbounded disk growth. Two independent policies are enforced:

### Age-based retention

Delete blocks with `ts_max` older than the cutoff:

```elixir
config :timeless_traces,
  retention_max_age: 7 * 86_400  # 7 days (default)
```

### Size-based retention

Delete oldest blocks until total size is under the limit:

```elixir
config :timeless_traces,
  retention_max_size: 512 * 1024 * 1024  # 512 MB (default)
```

### Disable retention

```elixir
config :timeless_traces,
  retention_max_age: nil,    # No age limit
  retention_max_size: nil    # No size limit
```

### Manual trigger

```elixir
TimelessTraces.Retention.run_now()
```

### Check interval

```elixir
config :timeless_traces,
  retention_check_interval: 300_000  # 5 minutes (default)
```

## Telemetry events

TimelessTraces emits telemetry events for monitoring:

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:timeless_traces, :flush, :stop]` | `duration`, `entry_count`, `byte_size` | `block_id` |
| `[:timeless_traces, :query, :stop]` | `duration`, `total`, `blocks_read` | `filters` |
| `[:timeless_traces, :retention, :stop]` | `duration`, `blocks_deleted` | |
| `[:timeless_traces, :block, :error]` | | `file_path`, `reason` |

See the [Telemetry](telemetry.md) guide for handler examples.

### Key metrics to monitor

| Metric | Source | Alert threshold |
|--------|--------|-----------------|
| Flush duration | `[:flush, :stop]` duration | Sustained > 100ms |
| Flush entry count | `[:flush, :stop]` entry_count | Sustained at max_buffer_size |
| Query latency | `[:query, :stop]` duration | > 5s for typical queries |
| Blocks read per query | `[:query, :stop]` blocks_read | Growing linearly |
| Block read errors | `[:block, :error]` | Any occurrence |
| Retention blocks deleted | `[:retention, :stop]` blocks_deleted | 0 when disk is growing |

## Troubleshooting

### High memory usage

- Check `raw_blocks` in stats -- many uncompacted raw blocks use more memory
- Trigger compaction: `TimelessTraces.Compactor.compact_now()`
- Reduce `max_buffer_size` to flush smaller batches
- Check for slow subscribers blocking the buffer

### Disk space growing

- Verify retention is configured: check `retention_max_age` and `retention_max_size`
- Trigger retention manually: `TimelessTraces.Retention.run_now()`
- Check stats for `total_bytes` trends
- Reduce retention age or size limits

### Slow queries

- Use `:service`, `:kind`, and `:status` filters to leverage the term index
- Avoid full scans (no filters) on large datasets
- Reduce the time range with `:since` and `:until`
- Check `raw_blocks` count -- many small raw blocks are slower to query than fewer compressed blocks

### Spans not appearing in queries

- Flush the buffer: `TimelessTraces.flush()`
- Check that the OTel exporter is configured: verify `traces_exporter: {TimelessTraces.Exporter, []}` in config
- Verify the data_dir exists and is writable
- Check for block read errors in telemetry events
- Check the `index_publish_interval` -- spans become queryable after the index batches them (default 2s)

### Compaction not running

- Check `raw_blocks` and `raw_entries` in stats
- Verify `compaction_threshold` isn't set too high for your span volume
- Trigger manually: `TimelessTraces.Compactor.compact_now()`
- Check that `compaction_max_raw_age` is reasonable (default: 60 seconds)

### OTel exporter not working

- Verify the dependency: `{:opentelemetry, "~> 1.5"}` must be in your mix.exs
- Check the config: `config :opentelemetry, traces_exporter: {TimelessTraces.Exporter, []}`
- Ensure spans are being created: use `OpenTelemetry.Tracer.with_span/2` in your code
- Check that TimelessTraces.Application has started (it must start before spans are exported)
