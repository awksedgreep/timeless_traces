# Storage & Compression

This document covers TimelessTraces' block-based storage format, compression options, and the compaction pipeline.

## Block-based storage

Spans are stored in **blocks** -- batches of spans written together and indexed as a unit. Each block contains:

- A batch of serialized spans (typically 500-2000)
- Block metadata: block_id, byte_size, entry_count, timestamp range (ts_min, ts_max), format
- An inverted index of terms for fast querying
- Trace index entries mapping trace IDs to the block

## Block formats

### Raw (`.raw`)

Uncompressed Erlang binary serialization (`:erlang.term_to_binary`). This is the initial format when spans are flushed from the buffer. Raw blocks are temporary -- the compactor merges them into compressed blocks.

### Zstd (`.zst`)

Erlang `term_to_binary` compressed with [Zstandard](https://facebook.github.io/zstd/). Simple, fast compression with good decompression speed.

| Metric | Value |
|--------|-------|
| Compression ratio | ~6.8x |
| Size (500K spans) | 32.8 MB |
| Configurable level | 1-22 (default: 6) |

### OpenZL (`.ozl`)

Columnar format with [OpenZL](https://github.com/nicholasgasior/ex_openzl) compression. Span fields are split into separate typed columns and each column is independently compressed. This exploits per-column redundancy for better ratios and enables selective column decompression at query time.

**Columnar layout:**

| Column | Encoding | Contents |
|--------|----------|----------|
| start_time | u64 packed | Start timestamps (nanoseconds) |
| end_time | u64 packed | End timestamps (nanoseconds) |
| duration | u64 packed | Duration (nanoseconds) |
| kind | u8 packed | Span kind (0-4) |
| status | u8 packed | Status (0-2) |
| trace_id | length-prefixed strings | Trace IDs |
| span_id | length-prefixed strings | Span IDs |
| parent_span_id | length-prefixed strings | Parent span IDs |
| name | length-prefixed strings | Span names |
| status_message | length-prefixed strings | Status messages |
| rest_blob | Erlang term_to_binary | Attributes, events, resource, scope |

| Metric | Value |
|--------|-------|
| Compression ratio | ~10.0x |
| Size (500K spans) | 22.3 MB |
| Configurable level | 1-22 (default: 6) |

### Choosing a format

The default is `:openzl` (columnar). Set via configuration:

```elixir
config :timeless_traces,
  compaction_format: :openzl,    # or :zstd
  compression_level: 6           # 1-22
```

| Use case | Recommended format |
|----------|-------------------|
| General use | `:openzl` (default) |
| Fastest queries | `:openzl` |
| Simpler compression | `:zstd` |

## Compaction

New spans are first written as raw (uncompressed) blocks for low-latency ingestion. A background Compactor process periodically merges raw blocks into compressed blocks.

### Compaction triggers

Compaction runs when any of these conditions are met:

1. **Entry threshold**: total raw entries >= `compaction_threshold` (default: 500)
2. **Age threshold**: oldest raw block >= `compaction_max_raw_age` seconds (default: 60)
3. **Manual trigger**: `TimelessTraces.Compactor.compact_now()`
4. **Periodic check**: every `compaction_interval` ms (default: 30,000)

### Compaction process

1. Read all raw block entries from disk
2. Merge entries into a single batch
3. Compress with the configured format (OpenZL or zstd)
4. Write a new compressed block file
5. Update the index (delete old block metadata, add new)
6. Delete old raw block files
7. Update compression statistics

### Compaction configuration

```elixir
config :timeless_traces,
  compaction_threshold: 500,       # Min raw entries to trigger
  compaction_interval: 30_000,     # Check interval (ms)
  compaction_max_raw_age: 60,      # Force compact after this many seconds
  compaction_format: :openzl,      # Output format
  compression_level: 6             # Compression level (1-22)
```

### Manual compaction

```elixir
TimelessTraces.Compactor.compact_now()
# => :ok or :noop (if nothing to compact)
```

## Disk layout

```
data_dir/
├── index.db          # SQLite index (WAL mode)
├── index.db-wal      # SQLite WAL file
├── index.db-shm      # SQLite shared memory
└── blocks/
    ├── 000000000001.raw   # Raw block (temporary)
    ├── 000000000002.raw   # Raw block (temporary)
    ├── 000000000003.ozl   # OpenZL compressed block
    ├── 000000000004.ozl   # OpenZL compressed block
    └── ...
```

Block filenames are 12-digit zero-padded block IDs with format-specific extensions.

## Memory storage mode

For testing or ephemeral environments, use in-memory storage:

```elixir
config :timeless_traces, storage: :memory
```

In memory mode:
- Block data is stored as BLOBs in an in-memory SQLite database
- No block files are written to disk
- The ETS tables still provide lock-free read access
- Data does not survive application restarts

## Compression statistics

Track compression efficiency via the stats API:

```elixir
{:ok, stats} = TimelessTraces.stats()

stats.compression_raw_bytes_in          # Total uncompressed bytes processed
stats.compression_compressed_bytes_out  # Total compressed bytes produced
stats.compaction_count                  # Number of compaction runs
```

The compression ratio is `compression_raw_bytes_in / compression_compressed_bytes_out`.

## Compression comparison

| Backend | Size (500K spans) | Ratio | Compress | Decompress |
|---------|-------------------|-------|----------|------------|
| zstd | 32.8 MB | 6.8x | 2.0s | 1.1s |
| OpenZL columnar | 22.3 MB | 10.0x | 2.0s | 2.3s |

OpenZL achieves better compression through columnar encoding -- timestamps compress well together, span kinds compress to nearly nothing, and string columns (names, IDs) benefit from prefix deduplication.
