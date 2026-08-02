# Rust traces API POC — Session 7 final verdict

Date: 2026-08-02  
Branch: `poc/rust-telemetry-data-plane`

## Verdict

**Keep the Rust traces data plane and advance it to artifact/release
promotion.** The POC preserves the complete declared rich-span and wire
contract, keeps `timeless_traces` as the sole storage engine, leaves the
extension's 8,192-span buffer authoritative, bounds read tails and process
memory, drains admitted work exactly, survives normal/abnormal process
lifecycle tests, and has honest SQLite maintenance behavior.

The existing Elixir block engine still wins the zero-query sustained write
control: 228.5K durable spans/s versus 178.5K for the fixed Rust HTTP run.
That 21.9% difference is accepted. The Rust run used 271,264 KiB HWM versus
811,812 KiB for the Elixir control at its last sub-100-ms write-p99 step, and
the reusable direct rich-v1 extension path remains faster at about 0.38M
spans/s. The API cost is OTLP validation, typed JSON preservation, batch
encoding, bounded admission, and HTTP—not a replacement block store.

The successful Session 7 implementation is committed and pushed directly,
with no PR and no merge:

- `timeless-libsql` `2f41170` — traces size-tiered/budgeted optimize,
  extension maintenance/gate telemetry, measured reader default, and Session
  7 benchmark harnesses.

## Storage boundary remained intact

Every measured write was one public rich-span v1 batch insertion into
`CREATE VIRTUAL TABLE traces USING timeless_traces`. The API has no span
buffer, compressor, block creator, shadow index, prune implementation, or
storage transaction grouping. A successful request covers its one SQLite
statement; the public flush barrier covers extension-buffer durability. The
extension still decides when its fixed 8,192-span threshold flushes and how
status-pure blocks, terms, and packed trace postings are encoded.

The declared release subset remains:

- OTLP/HTTP JSON, protobuf, and gzip protobuf at
  `/insert/opentelemetry/v1/traces`;
- Jaeger services, operations, trace lookup, and the established span-limited
  trace-search subset;
- lossless native dashboard span search and trace detail;
- health/readiness, exact storage/work telemetry, cancellation, and the
  completion-aware flush barrier.

It does not claim OTLP/gRPC, full Jaeger query semantics, auth, backup,
cluster control, links, trace state/flags, schema URLs, or dropped-count
fields. The last five span families remain the current product's documented
ingest omissions, not silent POC loss.

## Rewrite amplification found and fixed

The traces engine still had the pre-logs greedy optimizer. Sixteen successive
512-span flush/optimize cycles repeatedly rewrote the growing compressed tail:
101.45 ms cumulative optimize time and a 5.92 MB live physical footprint for
one final small logical block. A one-shot control needed only 12.68 ms. That
was sufficient evidence to port the already-proven logs policy rather than
assuming traces did not need it.

The reusable extension now separates raw compression from size-tiered
compressed merging. A compressed cohort is actionable only after reaching at
least half the 8,192 target and at least 2× its largest member; the 125% merge
ceiling and one-hour timestamp-span cap remain. Public
`optimize:<max_spans>` bounds one call while allowing one complete cohort to
exceed the budget so maintenance always makes progress.

The direct core regression is exact: 40 arrivals × 256 spans produce 10,240
spans while rewriting 22,528 total (2.2×), with only a deferred 2,048-span
tail. An uneven 30 × 307 regression pins the two legal merge tiers, and a
four-block regression proves two 512-span budgeted calls drain oldest raw
groups without loss. SQL integration pins the command and public stats keys.

The analogous 16 × 512 API probe, fully drained, reported:

| measure | greedy probe | size-tiered probe |
|---|---:|---:|
| cumulative optimize time | 101.45 ms | 33.75 ms |
| raw spans recompressed | included in growing rewrites | 8,192 |
| compressed spans merged | growing tail | 12,288 |
| final actionable backlog | 0 | 0 |
| live DB + WAL + SHM | 5.92 MB | 5.68 MB |
| checkpointed database | 327,680 B | 294,912 B |

The byte payloads in those two real probes were analogous rather than
byte-identical, so the exact regression is the rewrite-count proof; the wall
numbers are directional operational confirmation.

`timeless_stats('traces')` now exposes raw and merge groups/blocks/spans,
input/output bytes, phase time, bounded-call counts, actionable raw/merge
backlog, deferred compressed tails, and reader/writer-gate counters. The API
timer samples exact actionable blocks, converts a 32 MiB source-work target
to a span budget, and invokes the public command. It never plans blocks.

## Measured reader default

The fixed database has 800,000 spans in 95 optimized blocks. Every reader
configuration ran the same warmed 110-request sequence and returned exactly
1,140 traces, 1,300 spans, and 751,270 response bytes with no read error,
retry, or cancellation.

| readers | exact trace p95 | fan-out p95 | service+operation p95 | duration miss p95 | HWM |
|---:|---:|---:|---:|---:|---:|
| 1 | 4.81 ms | 42.97 ms | 33.33 ms | 378.84 ms | 44,768 KiB |
| **2** | **4.63 ms** | **32.90 ms** | **31.19 ms** | **362.43 ms** | **66,732 KiB** |
| 4 | 5.84 ms | 35.43 ms | 38.98 ms | 369.81 ms | 110,492 KiB |
| 8 | 7.00 ms | 36.52 ms | 34.59 ms | 376.19 ms | 197,956 KiB |

Two remains the default, now as a measured choice. Four and eight did not
improve this matrix and consumed 1.66×/2.97× the two-reader HWM. Discovery at
two readers was 0.90 ms p95 for both services and operations; the selective
time search was 34.39 ms p95.

The duration miss is the honest remaining tail. It must decode all candidate
blocks because persisted block metadata has no duration range. Session 5
already evaluated and rejected a duration-summary storage expansion because
it would add write/storage cost without satisfying the exact per-service
bucket contract.

## Direct extension versus HTTP

The final direct SQLite matrix used the same 800,000-span file and performed
the exact same engine work as the two-reader HTTP matrix: 5,900 candidates,
2,460 payload blocks, 39,974,500 logical payload bytes, 20.46M decoded spans,
1,688,420 matches, 1,300 returned spans, 80 bounded requests, and 3,440 blocks
skipped by bounds.

| shape | direct extension p95 | Rust HTTP p95 |
|---|---:|---:|
| services | 0.14 ms | 0.90 ms |
| operations | 0.19 ms | 0.90 ms |
| exact trace | 3.72 ms | 4.63 ms |
| service fan-out | 25.39 ms | 32.90 ms |
| service + operation | 24.08 ms | 31.19 ms |
| selective time | 31.86 ms | 34.39 ms |
| duration miss | 328.56 ms | 362.43 ms |

The direct process peaked at about 31.9 MiB; the two-reader server peaked at
65.2 MiB. These numbers show both that direct libSQL users receive the query
and optimizer work and that the remaining API/Jaeger assembly cost is bounded.

## Durable writes and fairness

Maintenance deferred, the fixed 16 × 100 × 500 HTTP run completed all 800,000
spans at 178,500 spans/s. The explicit barrier drained in 1.68 ms and reported
1,600 admitted/completed requests, zero failed spans, and queue/in-flight
`0/0`. Request p50/p95/p99 was 18.25/52.32/141.68 ms; HWM was 271,264 KiB.

For the short fixed mixed phases, each of zero/one/two broad-query phases
durably added exactly 100,000 spans:

| broad queries | durable spans/s | write p95 / p99 | query p95 | HWM | drain |
|---:|---:|---:|---:|---:|---:|
| 0 | 343,321 | 29.26 / 68.06 ms | — | 127,388 KiB | 6.20 ms |
| 1 | 449,935 | 38.57 / 68.59 ms | 409.29 ms | 158,316 KiB | 6.39 ms |
| 2 | 312,297 | 29.92 / 91.07 ms | 484.95 ms | 192,360 KiB | 6.44 ms |

The phases are deliberately short and the dataset grows, so their rates are
fairness evidence rather than peak-capacity rankings. The two-query phase was
9.0% below zero-query throughput, but all work completed and the extension
recorded zero writer waits, writer timeouts, read barge rejections, and read
retries. Therefore API admission fairness and host transaction grouping are
rejected for this POC: no measured publication problem exists.

With the bounded optimizer active every second, the same 800,000-span ingest
completed at 160,871 spans/s, a measured 9.9% maintenance tax. HWM was lower
at 200,156 KiB because raw payload was reclaimed during ingestion. Six
bounded extension calls drained all 800,000 raw spans with zero queue or
in-flight work; cumulative optimizer time was 1.063 seconds.

## Honest control comparison

The same-host Session 0 Elixir control and Session 7 Rust measurements use the
same deterministic 500-span request body and completion barrier, but the
Elixir test is a sustained ten-second rate ramp while the Rust fixed ingest
and mixed tests are bounded runs. They are shown side by side, not presented
as identical load generators.

| path | workload point | durable rate | process HWM |
|---|---|---:|---:|
| Elixir API / block store | zero query, last write-p99 <100 ms | **228.5K spans/s** | 811,812 KiB |
| Elixir API / block store | one query, last write-p99 <100 ms | 120.2K spans/s | 689,596 KiB |
| Elixir API / block store | two queries, last write-p99 <100 ms | 62.5K spans/s | 683,904 KiB |
| Rust API / libSQL | fixed 800K, zero query | 178.5K spans/s | **271,264 KiB** |
| Rust API / libSQL | fixed mixed 100K, two queries | 312.3K spans/s | **192,360 KiB** |
| direct rich-v1 extension | 960,570-span hostile fixture | ~380K spans/s | separate direct control |
| plain SQLite + trace index | same 960,570-span direct fixture | **840K spans/s** | separate direct control |

The existing block store's zero-query write lead is real. Conversely, at the
Elixir control's first 8K-spans/s step, service+operation, service fan-out,
trace lookup, and discovery were already 106.06/65.68/8.40/4.88 ms p95 with
two readers. The Rust fixed 800K database returns the corresponding shapes in
31.19/32.90/4.63/0.90 ms p95. At the Elixir two-query 62.5K step those tails
were 889.28/325.29/180.54/50.30 ms. Dataset and offered-load differences are
retained explicitly, but the read-side/process-boundary direction is clear.

The plain indexed table also honestly wins direct ingestion, 0.84M versus
0.38M rich-v1 spans/s on the Session 1 hostile fixture. Its 155,213,824-byte
file costs 161.59 bytes/span, while the optimized rich vtab used 35,790,848
bytes / 37.26 bytes/span—4.35× smaller while supplying its trace/term/block
query primitives. The POC chooses that storage/query trade, not a fabricated
universal SQLite win.

## Physical SQLite lifecycle

Logical maintenance, page reuse, checkpoint, expiration, and vacuum were
measured separately:

| operation | result |
|---|---:|
| 800K raw logical payload | 140,997,410 B |
| raw checkpointed database | 145,473,536 B |
| optimized logical payload | 1,562,832 B |
| optimized SQLite high-water / freelist | 146,784,256 / 137,494,528 B |
| reingest another 800K raw spans | freelist consumed; file grew only 4,636,672 B |
| offline `VACUUM` of optimized 800K file | 146,784,256 → 5,685,248 B in 24 ms |

Configured one-second data-time retention was exercised with two 8,192-span
epochs two seconds apart. The second public flush left exactly the new 8,192
spans, one block, the exact new min/max timestamp, and 1,490,944 freelist
bytes. Graceful shutdown checkpointed a 3,112,960-byte file; a separate
offline vacuum reduced it to 1,622,016 bytes in 5 ms. Reopen count/range and
`integrity_check` remained exact.

Thus optimize reduces logical payload and creates reusable pages; checkpoint
moves WAL state into the main file; expiration removes logical rows; page
reuse prevents regrowth; and only offline vacuum promises physical shrink.

## Verification

- `cargo test --workspace`: 69 core/library tests plus all integration and doc
  tests passed.
- full SQLite CLI suite: all 44 sections passed, including three randomized
  50K-operation oracle seeds and five rich-span kill-9 crash rounds.
- traces API: 9 library tests, 1 binary test, 4 Jaeger contracts, 4 OTLP/native
  dashboard contracts, and 4 storage/lifecycle contracts passed.
- strict POC Clippy passed; core/extension targeted Clippy passed with only the
  repository's pre-existing Rust-1.97 lint categories allowed.
- `timeless_traces`: formatting plus 183 tests passed.
- `timeless_traces_dashboard`: formatting plus 11 tests passed, including
  malformed/truncated response gates and real `SIGKILL` restart/graceful tail
  flush ownership.
- rich-span JSON/protobuf/gzip, 8,191/8,192 boundary, flush/optimize/reopen,
  cancellation/reuse, owner lease, backpressure, response parity, queue drain,
  and no-dangling-index regressions all passed.

## Exact next boundary

The POC is complete; it should not accrete auth or cluster administration on
this branch. The next fresh promotion branch should:

1. package and version the three Rust signal binaries with their compatible
   `timeless-libsql` extension artifact;
2. extract only the already-approved neutral OTP executable owner (startup,
   readiness, loopback endpoint, drain, restart, reap), keeping signal clients
   and routes separate;
3. put token issuance, auth policy, sessions, tenancy, configuration, and
   cluster routing in Phoenix/control-plane tables and APIs;
4. define backup/restore, schema/extension upgrade, rolling restart, and
   retention/vacuum operating procedures;
5. add sustained collector soak and resource-limit packaging gates; and
6. decide whether OTLP/gRPC belongs in the first public traces release or is a
   clearly documented later protocol.

Those are productization tasks, not evidence against the boundary. Session 7
meets its exit criterion and the traces POC is a keeper.
