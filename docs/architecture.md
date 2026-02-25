# Architecture

This document covers TimelessTraces' internal architecture: the supervision tree, data flow, storage format, and indexing strategy.

## Supervision tree

```
TimelessTraces.Supervisor (:one_for_one)
├── Registry (TimelessTraces.Registry)     # Pub/sub for live tail
├── Index (GenServer)                       # SQLite + ETS indexing
├── FlushSupervisor (Task.Supervisor)       # Async flush tasks
├── Buffer (GenServer)                      # Span accumulation
├── Compactor (GenServer)                   # Raw → compressed blocks
├── Retention (GenServer)                   # Age/size cleanup
└── HTTP (Bandit, optional)                 # OTLP ingest + Jaeger query
```

All components start automatically as an OTP application. The HTTP server is only started when `http` is configured.

## Data flow

```
OTel SDK
    ↓
Exporter (reads spans from OTel ETS table)
    ↓
Buffer (accumulate, auto-flush every 1s or 1000 spans)
    ↓ broadcasts to subscribers
Writer (serialize as raw Erlang terms)
    ↓
Disk (blocks/) or Memory (SQLite BLOB)
    ↓
Index (block metadata + inverted term index + trace index → SQLite + ETS)
    ↓
Compactor (merge raw → compressed OpenZL/zstd every 30s or at threshold)
    ↓
Retention (delete old/oversized blocks every 5 min)
```

## Exporter

The `TimelessTraces.Exporter` module implements the `:otel_exporter_traces` behaviour. When the OTel SDK calls `export/3`, the exporter:

1. Reads span records from the SDK's ETS table
2. Normalizes each span: converts trace/span IDs from integers to hex strings, extracts attributes, events, links, resource, and instrumentation scope
3. Converts timestamps from native units to nanoseconds
4. Ingests the normalized spans into the Buffer

This is a zero-copy path -- no HTTP, no protobuf serialization, no external collectors.

## Buffer

The Buffer is a GenServer that accumulates spans in memory. It flushes when:

- The buffer reaches `max_buffer_size` (default 1000 spans)
- The `flush_interval` timer fires (default 1 second)
- `TimelessTraces.flush()` is called manually

Before each flush, the Buffer broadcasts all spans to registered subscribers (live tail). Flush operations are dispatched as async tasks via the `FlushSupervisor` with backpressure -- at most `System.schedulers_online()` flushes run concurrently.

## Write path

Each flush writes a **raw block** -- a batch of spans serialized with `:erlang.term_to_binary`:

1. **Serialize**: Spans are converted to Erlang binary format
2. **Write**: The binary is written to a block file (`blocks/000000000001.raw`)
3. **Index**: Block metadata, inverted terms, and trace index rows are sent to the Index

The Index processes block metadata in batches (every `index_publish_interval` ms) to reduce SQLite write contention.

## Index

The Index uses a two-tier architecture for fast reads and durable writes:

### SQLite (persistence)

SQLite (WAL mode, 128MB mmap, 16MB cache) stores:

- **blocks** table: `block_id`, `file_path`, `byte_size`, `entry_count`, `ts_min`, `ts_max`, `format`, `data` (BLOB in memory mode)
- **block_terms** table: inverted index mapping terms to block IDs
- **trace_index** table: maps `trace_id` to block IDs with `span_count`, `root_span_name`, `duration_ns`, `has_error`
- **compression_stats** table: singleton row tracking compaction statistics

### ETS (fast reads)

Four ETS tables provide lock-free concurrent reads:

| Table | Type | Purpose |
|-------|------|---------|
| `timeless_traces_blocks` | ordered_set | Block metadata lookup |
| `timeless_traces_term_index` | bag | Term → block_id mapping |
| `timeless_traces_trace_index` | bag | trace_id → block_id mapping |
| `timeless_traces_compression_stats` | set | Compression statistics |

All ETS tables are created with `read_concurrency: true` and `write_concurrency: true`.

### Inverted term index

When a block is indexed, terms are extracted from each span and stored in the `block_terms` table. Terms include:

- `service:<name>` -- the service.name attribute or resource
- `kind:<kind>` -- span kind (server, client, etc.)
- `status:<status>` -- span status (ok, error, unset)
- `name:<span_name>` -- the span operation name

Queries use these terms to identify which blocks contain relevant spans, avoiding full scans.

### Trace index

The trace index maps each trace ID to the blocks that contain its spans. This enables fast trace lookup -- `TimelessTraces.trace(trace_id)` reads only the blocks that contain spans for that trace.

## Read path

A query follows this path:

1. **Term lookup**: Use the ETS term index to find block IDs matching the query filters
2. **Time range**: Further narrow blocks by timestamp range using the block metadata
3. **Parallel read**: Read and decompress matching blocks in parallel (`System.schedulers_online()` concurrency)
4. **Filter**: Apply in-memory filters to individual spans within each block
5. **Sort & paginate**: Sort by start_time (ascending or descending), apply offset and limit

## Compaction

The Compactor GenServer periodically checks for raw blocks and compresses them:

1. **Threshold trigger**: Raw entries >= `compaction_threshold` (default 500)
2. **Age trigger**: Any raw block older than `compaction_max_raw_age` (default 60 seconds)
3. **Periodic check**: Every `compaction_interval` ms (default 30,000)

The compaction process:

1. Read all raw block entries from disk
2. Merge entries into a single batch
3. Compress with the configured format (OpenZL columnar or zstd)
4. Write a new compressed block file
5. Update the index: delete old block metadata, add new
6. Delete old raw block files
7. Update compression statistics

## Merge compaction

After initial compaction produces many small compressed blocks (e.g. one per flush cycle), the Compactor runs a second pass that merges them into fewer, larger blocks. Larger blocks compress better (bigger dictionary window) and reduce per-block I/O overhead during reads.

1. Scan for compressed blocks with `entry_count < merge_compaction_target_size`
2. If enough small blocks exist (`>= merge_compaction_min_blocks`), group into batches by `ts_min`
3. For each batch: decompress all blocks, merge entries sorted by `start_time`, recompress
4. Update the index: remove old block metadata, add new merged block
5. Delete old compressed block files

The merge pass runs automatically after every compaction timer tick and can also be triggered manually via `TimelessTraces.merge_now()`.

| Configuration | Default | Description |
|---------------|---------|-------------|
| `merge_compaction_target_size` | 2000 | Target entries per merged block |
| `merge_compaction_min_blocks` | 4 | Minimum small blocks before merge triggers |

## Retention

The Retention GenServer enforces two independent policies every `retention_check_interval` ms:

1. **Age-based**: Delete blocks with `ts_max` older than `retention_max_age` seconds ago
2. **Size-based**: Delete oldest blocks until total storage is under `retention_max_size` bytes

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

Block filenames are 12-digit zero-padded block IDs with format-specific extensions (`.raw`, `.zst`, `.ozl`).
