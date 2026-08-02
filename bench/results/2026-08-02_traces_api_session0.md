# Rust traces API POC — Session 0 control and compatibility contract

Date: 2026-08-02

## Verdict

Session 0 passes. The existing Elixir HTTP/block-store service now has an
executable rich-span contract, an honest durability barrier, deterministic
load, and separate admission/completion/queue/failure counters.

The control's last point below the 100 ms write-p99 ceiling was:

| concurrent query workers | durable wall | write p99 | HWM at wall |
|---:|---:|---:|---:|
| 0 | 228.5K spans/s | 70.79 ms | 811,812 KiB |
| 1 | 120.2K spans/s | 13.78 ms | 689,596 KiB |
| 2 | 62.5K spans/s | 10.51 ms | 683,904 KiB |

All three final barriers reported admitted spans equal to completed spans,
zero failed spans, `queued_spans=0`, and `in_flight_spans=0`. These are
completed-work numbers, not successful-HTTP-response numbers.

Direct `timeless_traces` batch ingest in the current libSQL extension reached
0.78M spans/s and 37.36 bytes/span after optimize. It is already an excellent
core-span store, but its public ten-column shape cannot preserve the product's
rich spans. Session 1 must close that gap before a Rust HTTP server is useful.

## Method

Machine: Intel Core Ultra 9 185H, Linux, 22 schedulers. Client and server shared
the host. Each control run used a fresh data directory, production compilation,
16 writers, exactly 500 spans/request, seed `20260802`, 10-second offered
steps, a 3-second warmup, and zero/one/two deterministic query workers.

Maintenance was deliberately deferred with a high compaction threshold and
retention disabled. Each step:

1. captured the process-lifetime data-plane counters;
2. offered traffic for ten seconds;
3. stopped writers and readers;
4. called the flush barrier;
5. required the producer gauge and in-flight counters to reach zero; and
6. divided newly completed spans by offered time plus drain time.

The workload records route-shape p50/p95/p99, block/index bytes, final process
`VmRSS`, and process `VmHWM`. HWM is cumulative within each run; compare it at
the same ramp step, not across final datasets of different sizes.

The direct extension control used the existing deterministic 960,570-span,
100,000-trace `bench-traces` fixture and the public traces batch v0. Its ingest
time includes the extension's automatic 8,192-span flushes. The vtab result was
flushed, optimized, vacuumed, cold-reopened, and compared with a plain SQLite
table oracle.

## Durability defect found and fixed

The first rehearsal was rejected. The old `TimelessTraces.flush/0` could
return with 1,779 producer-gauge spans still queued:

```text
admitted_spans=104000 completed_spans=102221 queued_spans=1779
```

Two ordering gaps caused this:

- a `GenServer.call` from the flush requester can overtake casts sent by
  independent HTTP processes; and
- buffer workers publish block metadata to the index asynchronously, so a
  drained buffer was not necessarily an indexed durable block.

The barrier now captures the admitted-span target, drains every shard, syncs
the SQLite index, and repeats after a short yield until queued/in-flight work
is zero and completed plus failed spans cover the target. It raises on timeout
instead of returning a false success. A 32-producer regression proves that all
512 admitted spans are durably visible when the barrier returns.

`/health` and `/api/v1/flush` now expose additive counters:

- admitted requests, spans, and request bytes;
- drained requests at the last explicit barrier;
- durably completed and failed spans;
- rejected requests;
- queued spans and oldest queue age;
- in-flight flush batches and spans;
- raw compaction debt and overload state; and
- stored blocks/spans plus block and index bytes.

`drained_requests` is intentionally not named `completed_requests`. The
existing sharded block pipeline does not retain request identity through
storage. The barrier proves that all requests admitted before it have left the
pipeline, while completed/failed span counters state the storage outcome.

## Elixir control — zero readers

| offered interval | admitted spans/s | durable spans/s | drain | write p99 | HWM KiB |
|---:|---:|---:|---:|---:|---:|
| 1,000 ms | 8.0K | 8.0K | 3.79 ms | 4.78 ms | 345,996 |
| 500 ms | 15.9K | 15.9K | 29.82 ms | 5.69 ms | 517,640 |
| 250 ms | 31.8K | 31.6K | 35.33 ms | 5.86 ms | 600,288 |
| 125 ms | 63.0K | 62.9K | 12.19 ms | 5.87 ms | 600,288 |
| 62.5 ms | 124.4K | 124.0K | 29.63 ms | 9.59 ms | 636,496 |
| 31.25 ms | 229.3K | **228.5K** | 35.40 ms | 70.79 ms | 811,812 |
| 15.63 ms | 233.2K | 227.2K | 261.33 ms | 100.82 ms | 1,614,968 |

Final: 7,080,000 admitted/completed spans, zero failures, 6,776 raw blocks,
3,172,025,833 block bytes, 116,293,632 index bytes, empty queue.

## Elixir control — one reader

| offered interval | admitted spans/s | durable spans/s | drain | write p99 | query p95 | query p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 ms | 8.0K | 8.0K | 8.43 ms | 7.92 ms | 85.89 ms | 97.08 ms |
| 500 ms | 15.9K | 15.9K | 5.69 ms | 8.92 ms | 199.60 ms | 264.38 ms |
| 250 ms | 31.7K | 31.7K | 13.95 ms | 9.18 ms | 389.56 ms | 520.75 ms |
| 125 ms | 62.8K | 62.6K | 29.84 ms | 9.54 ms | 752.18 ms | 845.98 ms |
| 62.5 ms | 123.7K | **120.2K** | 291.55 ms | 13.78 ms | 1.21 s | 1.38 s |
| 31.25 ms | 155.6K | 145.8K | 668.94 ms | 746.97 ms | 1.85 s | 1.85 s |

Final: 4,001,000 admitted/completed spans, zero failures, 3,859 raw blocks,
1,792,563,464 block bytes, 63,815,680 index bytes, empty queue.

## Elixir control — two readers

| offered interval | admitted spans/s | durable spans/s | drain | write p99 | query p95 | query p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 ms | 8.0K | 8.0K | 11.84 ms | 11.89 ms | 77.28 ms | 106.06 ms |
| 500 ms | 15.9K | 15.9K | 6.46 ms | 10.93 ms | 223.47 ms | 257.62 ms |
| 250 ms | 31.7K | 31.6K | 26.50 ms | 9.28 ms | 422.73 ms | 528.34 ms |
| 125 ms | 62.6K | **62.5K** | 24.91 ms | 10.51 ms | 793.84 ms | 889.28 ms |
| 62.5 ms | 104.6K | 104.4K | 18.36 ms | 526.88 ms | 1.28 s | 1.40 s |

Final: 2,253,000 admitted/completed spans, zero failures, 2,198 raw blocks,
1,009,404,680 block bytes, 35,094,528 index bytes, empty queue.

## Query-shape tails

At 8K durable spans/s:

| route shape | one reader p95 / p99 | two readers p95 / p99 |
|---|---:|---:|
| service + operation search | 97.08 / 100.64 ms | 106.06 / 109.17 ms |
| service search | 60.20 / 64.09 ms | 65.68 / 68.28 ms |
| trace by ID | 3.23 / 9.33 ms | 8.40 / 10.47 ms |
| services discovery | 1.51 / 1.51 ms | 4.88 / 8.34 ms |

At approximately 62.5K durable spans/s:

| route shape | one reader p95 / p99 | two readers p95 / p99 |
|---|---:|---:|
| service + operation search | 845.98 / 845.98 ms | 889.28 / 889.28 ms |
| service search | 157.52 / 157.52 ms | 325.29 / 326.99 ms |
| trace by ID | 17.88 / 17.88 ms | 180.54 / 249.33 ms |
| services discovery | 9.50 / 9.50 ms | 50.30 / 50.30 ms |

The service+operation path is the first read target after parity. The result is
not a reason to alter Session 1 ordering: optimizing a schema that loses rich
span fields would optimize the wrong product.

## Direct libSQL core-span control

| path | ingest rate | file bytes | bytes/span | size vs plain |
|---|---:|---:|---:|---:|
| SQLite table + trace index | 0.78M spans/s | 155,213,824 | 161.59 | 1.0x |
| `timeless_traces` row inserts | 0.50M spans/s | 35,889,152 | 37.36 | 4.3x smaller |
| `timeless_traces` batch v0 | **0.78M spans/s** | same vtab format | 37.36 | 4.3x smaller |

Other timings on the cold-reopened vtab:

| operation | result | time |
|---|---:|---:|
| flush tail | — | 2.6 ms |
| optimize | — | 1,760.8 ms |
| count all | 960,570 | 587.2 ms |
| trace ID lookup, average of 100 | 936 spans total | 3.482 ms |
| status=error count | 10,220 | 4.3 ms |
| service + time-range count | 32,072 | 126.8 ms |
| `timeless_trace_buckets` | 500 rows | 419.1 ms |
| SQL GROUP BY fallback | 510 rows | 917.5 ms |

The oracle verified total rows, error count, service/range count, three
bit-exact ten-column spans, and one complete trace span set.

## Rich-span compatibility inventory

The checked fixture is `test/fixtures/data_plane/rich_trace.otlp.json`. The
black-box contract sends its semantic equivalent through OTLP JSON, OTLP
protobuf, and gzip protobuf, then validates the storage result, Jaeger result,
flush counters, and cold reopen.

### Preserved by the current Elixir product

| OTLP surface | behavior |
|---|---|
| trace/span/parent IDs | JSON strings retained; protobuf bytes lower-hex encoded |
| name and five span kinds | retained; unknown kind normalizes to `internal` |
| start/end time and duration | retained as nanoseconds; duration computed from endpoints |
| three status codes and description | retained |
| primitive span attributes | string, integer, double, and boolean retained |
| events | name, nanosecond timestamp, and primitive attributes retained |
| resource attributes | primitive values retained and used for Jaeger process/service |
| instrumentation scope | name and version retained |
| root/child relation | retained and rendered as Jaeger `CHILD_OF` |
| JSON/protobuf/gzip | accepted at the existing route with the current response body |

### Normalized or lossy in the current Elixir product

| OTLP surface | current behavior |
|---|---|
| JSON `intValue` | retains the JSON representation; standard string-encoded int64 is still a string |
| protobuf bytes attribute | base64 string |
| arrays/key-value-list attributes | inspected/stringified rather than structurally retained |
| duplicate attribute keys | last value wins when converted to a map |
| empty status description | normalized to `nil` on protobuf; JSON missing value is `nil` |
| service selection | span `service.name` wins over resource `service.name` |
| Jaeger scope | not rendered |
| search result | filters and limits spans before grouping; an operation search can return an incomplete trace |
| Jaeger `limit` | span limit, not trace limit |

The incomplete-search behavior is pinned in the contract rather than hidden.
Session 4 must either preserve it as compatibility or make an explicitly
tested product correction.

### Ignored by the current Elixir product

- trace state and flags;
- links;
- resource/scope schema URLs;
- instrumentation-scope attributes;
- dropped attribute/event/link counts;
- resource dropped-attribute count;
- event dropped-attribute count; and
- protobuf partial-success detail beyond the fixed empty response.

The POC may initially retain these established omissions, but it may not claim
full OTLP conformance.

### Jaeger query contract

The current routes expose services, operations, trace by ID, and trace search.
Search recognizes `service`, `operation`, `start`, `end`, `limit`,
`minDuration`, and `maxDuration`. Start/end are Jaeger microseconds converted
to nanoseconds. Duration strings accept `us`, `ms`, `s`, or a bare nanosecond
integer. Other Jaeger parameters are ignored.

## Verification

- `mix test`: 183 passed.
- rich HTTP/storage/Jaeger/cold-reopen contract: 5 passed.
- independent-producer durability regression: passed.
- direct extension deterministic oracle and cold-reopen benchmark: passed.
- formatting and `git diff --check`: passed.

Session 1 is unblocked. Its acceptance target is the exact rich-span fixture,
not merely the existing ten columns.
