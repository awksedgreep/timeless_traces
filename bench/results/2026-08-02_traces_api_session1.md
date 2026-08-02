# Traces API POC Session 1 — libSQL rich-span fidelity

Date: 2026-08-02

Session 1 is complete in `timeless-libsql` commits `368e204` (fidelity) and
`2176321` (completed-durable benchmark accounting) on
`poc/rust-telemetry-data-plane`. No server code and no alternate storage path
were added.

## Result

The public `timeless_traces` vtab now retains the complete span shape used by
the Session 0 contract:

- packed trace/span/parent IDs, name, service, kind, status, start, and duration;
- status description;
- typed and nested span attributes;
- typed events and event attributes;
- typed resource attributes; and
- instrumentation scope.

`attributes`, `resource`, and `instrumentation_scope` are public JSON object
columns. `events` is a public JSON array column. JSON is validated and
canonicalized at the SQLite boundary without converting values to strings.
There is no server-private fidelity blob.

Service indexing follows the established product precedence: string
`attributes["service.name"]`, then resource `service.name`, then the explicit
compatibility `service` column. Tests deliberately supply conflicting values
and prove both returned rows and `WHERE service = ...` use the derived winner.

## Compatibility

- Batch v0 remains byte `0x01` and keeps its original layout and flat-string
  attribute contract.
- Rich batch v1 is byte `0x02`; it retains the v0 prefix and appends status
  description, events, resource, and scope columns.
- The extension's 8,192-span automatic flush is unchanged and tested at
  8,191/8,192 using batch v1.
- New blocks use span block generation 2 with 14 columns.
- Generation-1 raw, zstd, columnar-4, and shredded columnar-5 blocks all have
  frozen compatibility writers in tests and decode successfully. Missing rich
  fields return `""`, `[]`, `{}`, and `{}`.
- Default rich values borrow static storage in memory; legacy/core spans do
  not pay millions of heap allocations merely to represent empty JSON.

The Session 0 compatibility decision remains explicit: links, trace state,
flags, schema URLs, and dropped-count fields remain ignored because the
current product does not retain them. Instrumentation-scope attributes remain
an established product omission even though the public scope JSON column can
carry them for direct users. Any future support requires additive public
schema and format work rather than a hidden blob.

The exact normalized values from
`test/fixtures/data_plane/rich_trace.otlp.json` are independently inserted by
both SQL rows and batch v1. A reference projection pins the established Jaeger
tag/log types, status-description tag, event timestamp conversion, and event
fields from the values read back from SQLite.

## Direct ingest and storage

Deterministic fixture: 960,570 spans / 100,000 traces, automatic 8,192-span
flush, transaction commit and final explicit flush included, caller batch
encoding excluded exactly as in the Session 0 direct control. Each 50K-span
batch crosses the extension threshold, so the measured final batch flush is a
0.0 ms no-op; all admitted spans are already durable at commit. Rates are the
median of three final runs. File sizes are after optimize and stepped
incremental vacuum.

| path | direct ingest | optimized bytes | bytes/span | relative size |
|---|---:|---:|---:|---:|
| plain SQLite + trace index | 0.84M spans/s | 155,213,824 | 161.59 | 4.35x rich v1 |
| vtab row/core | 0.50M spans/s | 35,700,736 | 37.17 | — |
| batch v0/core | 0.61M spans/s | 35,078,144 | 36.52 | 1.000x |
| batch v1/rich | 0.38M spans/s | 35,790,848 | 37.26 | 1.020x |

Rich fidelity costs **0.74 bytes/span / 2.0%** on this fixture. Batch-v1
ingest is 37.7% slower than the same-run batch-v0 control because it validates
and canonicalizes five typed JSON/text columns. Against the Session 0
historical v0 result (0.78M spans/s), the current v0 path is 21.8% slower;
this is recorded as a real compatibility cost, not normalized away. It still
completes durable direct ingestion at about 610K spans/s.

The row path recovered to 0.50M spans/s after borrowed defaults, matching the
historical 0.50M result. Median final row flush was 2.5 ms and median row
optimize was 1,915.8 ms. In the last fully reported run, batch-v0 optimize was
2,047.4 ms and batch-v1 rich optimize was 2,134.1 ms.

## Read tails and memory

Warm distributions below query independently reopened, optimized direct-batch
databases. HWM is the Linux process `ru_maxrss`; each format ran in its own
process. The current vtab still materializes complete query results before
SQLite consumes them, so wide/full memory is intentionally visible here for
Session 5 rather than hidden.

| format / query | p50 | p95 | p99 |
|---|---:|---:|---:|
| v0 exact trace, n=100 | 18.869 ms | 21.314 ms | 23.292 ms |
| v1 exact trace, n=100 | 26.728 ms | 28.809 ms | 30.352 ms |
| v0 error count, n=30 | 4.413 ms | 4.591 ms | 4.778 ms |
| v1 error count, n=30 | 5.378 ms | 5.619 ms | 5.930 ms |
| v0 service/range, n=30 | 157.418 ms | 200.734 ms | 216.942 ms |
| v1 service/range, n=30 | 198.839 ms | 227.623 ms | 227.769 ms |
| v0 full count, n=8 | 617.080 ms | 643.995 ms | 643.995 ms |
| v1 full count, n=8 | 667.069 ms | 703.277 ms | 703.277 ms |

- v0 read-process HWM: 561,944 KiB.
- rich-v1 read-process HWM: 658,692 KiB.
- maximum combined Rust generator/ingest/read benchmark HWM: 901,960 KiB; this is not
  an embedded-server target because it includes the full generated fixture
  and the known materializing read path.

Rich payload decoding currently adds roughly 20–41% to these query shapes and
17.2% to the isolated read HWM. The later stable-snapshot/streaming session is
still necessary.

## Regressions and verification

- Exact Session 0 row-versus-batch fixture, semantic JSON, service precedence,
  Jaeger tag/log reference projection, 8,191/8,192 threshold, malformed shape,
  truncated batch atomicity, savepoint rollback, flush, optimize, prune,
  corrupt block failure, cold reopen, and `integrity_check`: passed.
- Generation-1 compatibility across all four historical codec IDs: passed.
- `cargo test --workspace`: passed (170 tests plus doc tests).
- Randomized rich plain-table oracle: three seeds × 50,000 operations; 7,523
  query comparisons, 2,437 transactions, and 139 prune-all cycles passed.
- Statement/savepoint/auto-flush/maintenance rollback suite: passed.
- Five independent random-timing `kill -9` rounds: every watermark held, every
  surviving rich span retained typed JSON, integrity checks passed, and no
  term/trace-index row dangled.
- Full CLI integration suite passed before the borrowed-default-only memory
  refinement; the affected rich regression, workspace suite, three oracle
  seeds, rollback suite, and crash suite were rerun afterward and passed.

## Exit criterion

Satisfied. One hundred percent of the Session 0 rich fixture survives direct
SQLite row and public batch-v1 ingest, flush, optimize, and cold reopen. Batch
v0 and every existing block generation remain readable, and no Rust HTTP
server exists yet.
