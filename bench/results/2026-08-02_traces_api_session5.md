# Rust traces API POC — Session 5

Date: 2026-08-02  
Branch: `poc/rust-telemetry-data-plane`

## Verdict

Session 5 passes. The read bottleneck was not inherent to libSQL: it was the
extension returning a fully materialized database-sized span vector before
SQLite could apply `ORDER BY`, `LIMIT`, `DISTINCT`, duration rechecks, or JSON
grouping. The reusable extension boundary now owns those mechanics.

On the fixed 800K-span route sequence, API read time fell from 53.049 seconds
to **9.790 seconds (5.42×)** and extension query time fell from 36.593 seconds
to **9.740 seconds (3.76×)**. Process HWM fell from 1,115,040 KiB to **324,652
KiB (−70.88%)**. The expensive service fan-out and service/operation searches
improved 25.71× and 19.32× at p95; discovery is now sub-millisecond. The one
honest holdout is a duration miss: it improved 1.62× to 409.26 ms p95 but must
still decode every candidate block because generation-1 block metadata has no
duration range.

## Reusable extension changes

- `SpanBlockStore` now advertises stable query locations. SQLite-backed spans
  retain only candidate row ids under the transition guard; the outer host
  statement pins old WAL/rollback-journal row versions while payloads stream.
  Conservative stores own payload bytes before releasing the guard.
- Unbounded `timeless_traces` cursors decode at most one block at a time and
  expose one current row. Early SQLite termination cannot strand a complete
  result vector in Rust.
- `xBestIndex` recognizes exact `ORDER BY start_ts[,span_id] ASC|DESC LIMIT /
  OFFSET`. The engine keeps only `LIMIT + OFFSET` spans in a bounded heap and
  stops when later block timestamp bounds cannot displace the retained prefix.
  Strict or unknown rechecks visibly disable the bounded plan.
- Inclusive `duration_ns` bounds are exact engine predicates. This prevents
  duration candidates from crossing the vtab even when every block must still
  be decoded.
- Public `timeless_trace_services('traces')` and
  `timeless_trace_operations('traces', service)` TVFs read posting-list
  metadata. New blocks add an `operations:` marker and collision-free,
  length-prefixed service/operation terms. Mixed legacy/new databases fall
  back to exact block-at-a-time decode rather than omitting values.
- `timeless_trace_buckets` now aggregates from the streaming snapshot. Exact
  p50/p95/p99 retain only duration vectors, not rich span structs, and its read
  permit is released before decode and percentile sorting.
- `timeless_stats('traces')` exposes snapshot time/owned bytes, stable-location
  count, bounded requests/max/skipped blocks, native discovery work, and the
  existing payload/decode/match/return/cancellation counters.

No storage path was bypassed or recreated. Writes still cross the public rich-
span batch and the extension remains the sole owner of its **8,192-span**
buffer, block encoding, term/trace indexes, flush, optimize, and prune.

## Correctness and regressions

The Session 0 rich fixture still matches every Jaeger field and wire decision.
New regressions pin:

- bounded ASC/DESC ordering, packed-span-id ties, OFFSET capacity, block-bound
  early termination, and exact duration-before-limit behavior;
- streaming cursor memory and stable-location publication;
- flush, optimize, and prune overtaking post-snapshot materialization without
  missing, duplicating, or reading deleted rows;
- metadata-native services/operations including the live buffer;
- exact legacy-operation fallback;
- SQLite planner selection and public discovery SQL;
- real SQLite writer publication while a 131,072-span duration miss is still
  decoding;
- reader cancellation/reuse after discovery became too fast to serve as the
  cancellation workload; and
- the two boundary defects found during validation: `xNext` must bind the host
  connection before streamed reads, and a generated `WHERE 1=1` tautology
  prevents SQLite from selecting the bounded vtab plan.

## Fixed 800K ingest and storage

Fresh database, release build, retention/scheduled maintenance effectively
disabled, two readers, and the same deterministic 16 × 100 × 500 fixture used
in Session 4. Every 200 response covered its SQLite statement; explicit flush
covered all 1,600 requests and 800,000 spans with zero failures, queued work,
or in-flight work.

| measure | Session 4 | Session 5 | change |
|---|---:|---:|---:|
| durable completed throughput | 186,760.8 spans/s | 183,868.9 spans/s | −1.55% |
| write p50 | 15.39 ms | 16.60 ms | +7.9% |
| write p95 | 21.70 ms | 43.19 ms | +99.0% |
| write p99 | 130.63 ms | 142.90 ms | +9.4% |
| logical block payload | 140,997,410 B | 140,997,410 B | unchanged |
| pre-checkpoint DB+WAL+SHM | 277,129,872 B | 277,408,856 B | +0.10% |
| checkpointed database | 145,408,000 B | 145,473,536 B | +64 KiB |
| term rows | 570 | 855 | +285 discovery rows |

The write change is retained: completed throughput is within 1.6%, logical
payload is byte-identical, and the exact operation catalog costs 0.05% in the
checkpointed file. Request p95 was noisier in this single run, so it remains a
Session 7 sweep item rather than being hidden behind the throughput average.

## Fixed read comparison

Each plan was warmed once; then 110 measured requests used the exact Session 4
shape and response cardinality.

| route shape | Session 4 p95 | Session 5 p95 | improvement |
|---|---:|---:|---:|
| exact trace, 5 spans | 6.62 ms | 5.64 ms | 1.17× |
| selective time window | 29.77 ms | 26.74 ms | 1.11× |
| duration miss | 663.04 ms | 409.26 ms | 1.62× |
| service + operation | 697.72 ms | 36.11 ms | 19.32× |
| services discovery | 703.62 ms | 0.57 ms | 1,225.85× |
| operations discovery | 838.98 ms | 0.76 ms | 1,107.09× |
| service fan-out | 982.68 ms | 38.22 ms | 25.71× |

Responses stayed exactly 1,140 traces / 1,300 spans / 751,270 bytes. Work for
the 110 requests changed as follows:

| extension/API work | Session 4 | Session 5 | reduction |
|---|---:|---:|---:|
| API read wall | 53.049 s | 9.790 s | 81.55% |
| extension query CPU/wall | 36.593 s | 9.740 s | 73.38% |
| candidate blocks | 6,810 | 5,840 | 14.24% |
| payload blocks read | 6,810 | 2,400 | 64.76% |
| logical payload bytes read | 10,109,109,180 | 3,516,287,200 | 65.22% |
| decoded spans | 57,360,000 | 19,950,000 | 65.22% |
| matched spans crossing vtab | 43,232,420 | 1,688,420 | 96.09% |
| bounded queries / requested spans | — | 80 / 1,600 | — |
| blocks skipped by bound | — | 3,440 | — |
| native discovery calls / payload reads | — | 10 / 0 | — |
| max owned snapshot payload | — | 0 B | — |
| read errors/retries/cancellations | 0 / 0 / 0 | 0 / 0 / 0 | exact |

The final query process ended at 324,652 KiB RSS/HWM. Session 4 ended at
855,816 KiB RSS and 1,115,040 KiB HWM. Session 5's process was restarted on the
checkpointed fixture before measurement, so the HWM is read-sequence-local;
the separate ingest HWM was 246,216 KiB, below the read HWM.

## Mixed writer/read fairness

On the same process, each phase durably added 100,000 spans (8 writers × 25 ×
500) while zero, one, or two broad duration-miss queries ran. The dataset grows
between phases; this deliberately makes later reads harder.

| concurrent broad queries | durable spans/s | write p95 | query p95 | HWM |
|---:|---:|---:|---:|---:|
| 0 | 326,395 | 37.52 ms | — | 405,252 KiB |
| 1 | 327,814 | 32.94 ms | 457.74 ms | 405,252 KiB |
| 2 | 317,933 | 31.36 ms | 651.99 ms | 445,132 KiB |

Two broad queries reduced durable writer throughput by only 2.59% versus the
zero-query phase. All 300,000 spans completed and flushed; there were no write
or query failures. This is the direct proof that query CPU no longer owns the
publication gate.

## Bucket evaluation and rejected metadata expansion

The exact trace-bucket path was material because it previously constructed a
rich 800K-span vector and then separate duration vectors. Streaming fixed that
without changing storage: on the grown 1.1M-span mixed database, 19 exact
bucket rows covering all spans completed in 0.473 seconds with a standalone
SQLite process HWM of **35,464 KiB**.

Per-block duration aggregates were evaluated and rejected for this session.
Min/max metadata could prune a duration miss, but it cannot answer the public
bucket contract's per-service span/error counts or exact p50/p95/p99. Adding a
new persisted summary/sketch format would increase write/storage cost and
change exact semantics for a TVF not used by the POC API. The measured
duration-miss tail remains explicitly decode-bound for future work.

No packed trace frame was added. Streaming is the smaller public primitive:
ordinary SQLite queries benefit directly, bounded searches never hold more
than the requested prefix, and scalar SQL aggregates consume the cursor
without an extension-side rowset.

## Verification

```text
cargo test --workspace
  178 Rust tests plus doc tests passed

bash tests/cli.sh
  all 44 sections passed
  3 randomized 50K-operation oracle seeds passed
  5 rich-span kill-9 crash rounds passed

cd poc/timeless-traces-api
cargo test --all-targets
  9 unit tests + 11 extension-backed contract tests passed
cargo clippy --all-targets -- -D warnings
  passed

targeted timeless-core/timeless-ext library Clippy
  passed with the repository's pre-existing Rust-1.97 lint categories allowed
```

## Next

Session 6 crosses one real Phoenix dashboard historical-search and trace-detail
path through this process, then proves normal and abnormal OTP child ownership.
