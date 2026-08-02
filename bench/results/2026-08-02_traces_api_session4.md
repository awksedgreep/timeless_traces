# Rust traces API POC — Session 4

Date: 2026-08-02  
Branch: `poc/rust-telemetry-data-plane`

## Verdict

Session 4 passes. All four pinned Jaeger routes now run socket-to-response in
Rust over the public `timeless_traces` vtab. The Session 0 rich fixture matches
the control's semantic envelopes, service/operation discovery, complete trace
shape, typed tags, process/resource tags, status description, event logs,
timestamps, duration units, and parent references.

The 800K-span fixed read baseline also makes Session 5's need unambiguous.
Exact trace lookup is already strong at **6.62 ms p95**, but discovery and
broad search remain 0.62–0.98 seconds p95 and drove process HWM to **1,115,040
KiB**. Those broad queries decoded 57.36M spans and read 10.11GB of logical
payload to return 1,300 spans across 110 measured requests. This is a measured
extension materialization problem, not HTTP or Jaeger JSON speculation.

## Compatibility decisions

- `GET /select/jaeger/api/services` returns a sorted distinct list in the
  established `{data,errors,limit,offset,total}` envelope.
- `GET /select/jaeger/api/services/:service/operations` returns sorted exact
  operation names for that service.
- `GET /select/jaeger/api/traces/:trace_id` uses the public trace-ID constraint,
  assembles all indexed blocks plus the live buffer, and returns deterministic
  `(start_ts, span_id)` span order.
- Search supports `service`, `operation`, Jaeger-microsecond `start`/`end`,
  `limit`, and `us`/`ms`/`s` or bare-nanosecond duration bounds.
- The established product behavior is retained: search orders newest first,
  applies `limit` to spans, then groups those spans into traces. It may return
  an incomplete trace. This is compatibility, not a standards claim.
- Unknown Jaeger parameters remain ignored as inventoried in Session 0.
  Malformed duration and negative-limit input now returns a visible 400 rather
  than crashing or silently broadening a query.
- Instrumentation scope remains stored but not rendered, matching the current
  product. Links, flags, trace state, schema URLs, and dropped counts retain
  the previously documented ingest omissions.

## Fidelity and regressions

The Rust differential contract ingests the exact Session 0 OTLP JSON fixture,
flushes it, and compares the pinned semantic oracle:

- services: `contract-svc`;
- operations: `DB contract`, `GET /contract`;
- root/child IDs and `CHILD_OF` reference;
- nanoseconds converted to Jaeger microseconds;
- `server`/`client` kind and `ERROR`/`UNSET` status tags;
- typed string/int64/float64/bool span and process tags;
- status-description tag;
- exception event timestamp/name/typed fields; and
- operation search returning only the root span under the established
  span-limit/filter-before-grouping contract.

The rich fixture's error and unset spans naturally occupy different status-
pure extension blocks, so its trace detail already proves cross-block assembly.
A separate 8,193-span regression forces the same target trace into two
successive threshold blocks and verifies complete deterministic root/child
assembly and parent fidelity.

Cancellation is scoped per request. The API installs and always clears a
SQLite progress handler, retains an interrupt handle for the selected reader,
and calls `sqlite3_interrupt` when the HTTP future is dropped. The public
SQLite-backed span store observes interruption at its block-work checkpoints;
the host row/render loops independently check the request flag. A 131,072-span
extension-backed regression cancels discovery during work, observes exactly
one API and one extension cancellation, and successfully runs another query
on the same sole reader.

## Fixed Session 4 baseline

Fresh database, release build, retention and scheduled maintenance disabled,
two readers, deterministic `bench/ingest.py` fixture: 16 writers × 100 requests
× 500 spans = 800,000 spans. Every HTTP 200 covered its one SQLite statement;
the final ordered flush reported 1,600 admitted/completed requests, 800,000
admitted/completed spans, and zero failed/queued/in-flight work.

Ingest completed at 186.8K durable spans/s. Request p50/p95/p99 was
15.39/21.70/130.63 ms. This fixed run is consistent with Session 3's separate
800K phase-attribution run (183.7K spans/s, 260,036 KiB HWM); the current run
was retained primarily as the exact Session 4 read fixture.

The flushed raw store contained 95 blocks, 140,997,410 logical payload bytes,
and 277,129,872 physical database+WAL+SHM bytes before graceful checkpoint.

Warm route timings on that fixed database:

| route shape | requests | p50 | p95 | p99 | returned |
|---|---:|---:|---:|---:|---:|
| exact trace, 5 spans | 20 | 5.21 ms | 6.62 ms | 6.62 ms | 1 trace / 5 spans |
| selective time window | 20 | 28.69 ms | 29.77 ms | 29.77 ms | 20 traces / 20 spans |
| duration miss | 20 | 615.49 ms | 663.04 ms | 663.04 ms | empty |
| service + operation | 20 | 671.37 ms | 697.72 ms | 697.72 ms | 20 traces / 20 spans |
| services discovery | 5 | 702.10 ms | 703.62 ms | 703.62 ms | 1 service |
| operations discovery | 5 | 794.31 ms | 838.98 ms | 838.98 ms | 2 operations |
| service fan-out | 20 | 946.99 ms | 982.68 ms | 982.68 ms | 16 traces / 20 spans |

The query process ended the sequence at 855,816 KiB RSS and 1,115,040 KiB
HWM. HWM is process-lifetime and includes the preceding ingest plus warmup,
but its jump well beyond the comparable Session 3 ingest-only 260,036 KiB
directly identifies broad read materialization.

## Work attribution

For the 110 measured route calls:

| counter | delta |
|---|---:|
| API read wall time | 53.049 s |
| API reader/SQLite time | 53.033 s |
| extension query time | 36.593 s |
| extension candidate/payload blocks | 6,810 / 6,810 |
| extension payload bytes read | 10,109,109,180 |
| extension decoded spans | 57,360,000 |
| extension matched/returned-to-SQL spans | 43,232,420 |
| API returned traces/spans | 1,140 / 1,300 |
| encoded HTTP response bytes | 751,270 |
| read errors/retries/cancellations | 0 / 0 / 0 |

The extension and API counts differ deliberately. Duration is not yet an
engine constraint, and SQL `LIMIT` cannot prevent the current vtab cursor from
materializing its complete engine result. The exact fixture regression even
pins one max-duration candidate returned by the extension and rejected by
SQLite. These counters establish Session 5's optimization order:

1. bounded order/limit and streaming/stable snapshots;
2. metadata-native discovery;
3. duration pushdown for measured broad duration shapes; and
4. reader-count sweeps only after result shape and memory are bounded.

## Verification

```text
cargo test --workspace
  171 Rust tests plus doc tests passed

bash tests/cli.sh
  all 43 sections passed
  3 randomized 50K-operation oracle seeds passed
  5 rich-span kill-9 crash rounds passed

cd poc/timeless-traces-api
cargo test --all-targets
  9 unit tests + 10 extension-backed contract tests passed
cargo clippy --all-targets -- -D warnings
  passed
```

## Next

Session 5 fixes the measured extension read path for all direct SQLite/libSQL
users. It will keep or revert each optimization independently and repeat this
exact 800K-span route sequence after every meaningful change.
