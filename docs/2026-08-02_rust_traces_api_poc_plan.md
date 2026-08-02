# Rust traces API POC plan

Date: 2026-08-02
Status: Sessions 0–4 complete; Session 5 ready on `poc/rust-telemetry-data-plane`

This POC tests the same process boundary that succeeded for logs and metrics:
Rust owns the telemetry HTTP/data plane, while Elixir/Phoenix owns product
state, sessions, dashboards, and cluster administration. It does **not** build
a new traces storage engine. The existing `timeless_traces` virtual table in
`timeless-libsql` remains the only candidate storage/query engine.

The POC is traces-specific. It will be named `timeless-traces-api`; a unified
three-signal daemon is a decision for after all three signal implementations
show which code is genuinely common.

## Boundary under test

```text
OTel SDK / Collector / VictoriaTraces clients       Jaeger UI
                     |                                  |
              OTLP JSON/protobuf                 Jaeger HTTP
                     +---------------+------------------+
                                     v
                          timeless-traces-api
                    parse, bounded admission, query
                    planning, response encoding,
                    cancellation, API telemetry
                                     |
                                     v
                    public timeless-libsql surfaces
                                     |
                                     v
                          traces.db (one owner)

Phoenix / LiveView / TimelessTracesDashboard / Stack
sessions, auth policy, UI state, configuration, cluster administration
                                     |
                         loopback HTTP or Unix socket
                                     v
                          timeless-traces-api
```

The existing in-process `TimelessTraces.Exporter` remains the control path
during the POC. A successful daemon gives applications and collectors a
standard OTLP endpoint; it does not require removing the embedded exporter in
the same change.

## Lessons carried forward

### Keep

1. **Baseline the real service before server code.** Admission is not durable
   completion. Pin exact request/response fixtures, completed work, drain, and
   current process HWM before changing the boundary.
2. **Do not reproduce storage for a quick benchmark.** The API reaches traces
   only through the released extension, its batch format, and public query
   surfaces.
3. **Keep the engine's batching contract.** The extension owns the 8,192-span
   automatic flush. One accepted OTLP request becomes one established batch
   insertion; the API must not add a 1,000-span buffer, compress individual
   requests, issue per-span SQL, or turn request boundaries into block
   boundaries.
4. **Fix the lowest reusable layer.** Writer fairness, stable query snapshots,
   bounded ordered reads, scalar reductions, trace lookup, compaction policy,
   and storage telemetry belong in `timeless-core`/`timeless-libsql` whenever
   direct SQLite/libSQL users benefit.
5. **Measure protected time and total memory.** Logs showed that releasing a
   writer guard can fix throughput while whole-result materialization still
   consumes gigabytes. Both are release gates.
6. **Bound result shapes before tuning reader count.** `ORDER BY ... LIMIT`,
   scalar counts, and exact filtering removed database-sized logs results.
   Reader sweeps only became meaningful afterward.
7. **Maintenance is a separate workload.** Measure ingest with maintenance
   deferred, then compaction/retention under load, drain-to-zero, logical
   payload, SQLite high-water/reuse, and explicit vacuum separately.
8. **Concurrency follows evidence.** One/two/four/eight readers are measured
   again for traces. Two is a provisional correctness default, not an answer
   copied from logs or metrics.
9. **Keep lifecycle ownership explicit.** One process owns the database;
   graceful `SIGTERM` drains and flushes, abnormal death is isolated, OTP can
   restart the child, and incomplete responses are never partial UI data.
10. **Keep the POC narrow and honest.** No auth implementation, backup UI,
    cluster protocol, or generic server framework is allowed to distract from
    storage fidelity and the API boundary.

### Do not copy blindly

- Logs' level partition and exact substring/count primitives are not trace
  query semantics.
- Metrics' PromQL parser, grid/window frames, and 4,096-point series buffers
  do not apply to traces.
- The traces reader count, query mix, memory profile, and maintenance cadence
  need their own measurements.
- A trace search returns traces assembled from spans; applying a span limit
  before grouping is not automatically the same as a Jaeger trace limit.

## Current reality and the first blocking gap

The current Elixir product and the current extension do not yet store the same
span shape.

| capability | current `timeless_traces` product | current libSQL vtab |
|---|---|---|
| IDs | trace/span/parent hex strings | packed BLOBs, hex accepted at input |
| core fields | name, kind, start/end/duration, status | name, service, kind, start/duration, status |
| span attributes | typed map | flat JSON converted to string pairs |
| status description | preserved | absent |
| events | preserved and rendered as Jaeger logs | absent |
| resource attributes | preserved and used for processes/service | absent except explicit `service` |
| instrumentation scope | preserved | absent |
| trace lookup | packed trace index in SQLite | packed `_trace_blocks` index |
| ingest threshold | product default 1,000 spans / 1 second | fixed 8,192 spans |
| ingest batch | Elixir maps through buffer/shards | public columnar batch v0 |
| compression | OpenZL/zstd block files | status-pure timeless codec blocks |
| transactions/recovery | product block/index protocol | SQLite transactions and recovery |

The extension already has the right skeleton: public batch ingest, an 8,192
automatic flush, buffered visibility, status-pure blocks, a packed trace index,
term and time-range pruning, retention, transactions/savepoints, crash-tested
recovery, and `timeless_trace_buckets`. It also has measured direct ingest near
1M spans/s and 37.4 bytes/span on the hostile 960K-span fixture.

However, an HTTP server over the current ten-column vtab would silently lose
status descriptions, events, resources, scope, and attribute value types. That
would make a fast benchmark meaningless. Session 1 closes this fidelity gap
before the server accepts OTLP.

OTLP fields the current Elixir product already ignores or does not expose—such
as links, trace state, flags, schema URLs, and dropped-count fields—must be
listed explicitly in the Session 0 compatibility inventory. The POC may retain
the current behavior, but it may not silently claim full OTLP conformance.

## Fixed storage and API contracts

- `CREATE VIRTUAL TABLE traces USING timeless_traces` remains the storage
  boundary. The server may not create a parallel block/index implementation.
- The extension's 8,192-span automatic flush remains authoritative. Explicit
  flush, optimize, prune, buffered visibility, transactions, and restart
  behavior remain public SQL contracts.
- OTLP JSON, OTLP protobuf, protobuf gzip, the 10 MiB request cap, and the
  current Jaeger routes are in scope because the existing HTTP service exposes
  them today.
- Trace/span/parent IDs, nanosecond timestamps, duration, kind, status, status
  description, typed attributes, events, resource, and instrumentation scope
  must survive ingest, flush, optimize, reopen, query, and Jaeger rendering.
- One HTTP request is parsed once, encoded once into a versioned columnar batch,
  and admitted once to a bounded writer. No per-span SQL path is a production
  benchmark path.
- One ordered SQLite writer owns mutations. Reads use a bounded configurable
  pool. The API reports admission, completion, failure, queue depth/age, and
  drain independently.
- The Rust process is the sole owner of its `traces.db`. Controls use separate
  fresh data directories/databases.
- API cancellation must stop host work and SQLite work, clear progress state,
  and leave the reader reusable.
- Every optimization is additive and independently keep/reject measurable.
  Exactness beats a favorable benchmark.

## Compatibility surface to pin

### Ingest

- `POST /insert/opentelemetry/v1/traces`
  - OTLP JSON;
  - OTLP protobuf;
  - gzip-compressed protobuf;
  - nested resource/scope/span/event attributes;
  - root and child spans;
  - all five span kinds and three status values;
  - empty requests, malformed payloads, invalid IDs/timestamps, partial input,
    wrong content type, oversized body, and decompression failure;
  - exact `ExportTraceServiceResponse`/JSON status and body semantics.

### Query

- `GET /select/jaeger/api/services`
- `GET /select/jaeger/api/services/:service/operations`
- `GET /select/jaeger/api/traces/:trace_id`
- `GET /select/jaeger/api/traces` with service, operation, start/end, limit,
  minDuration, and maxDuration
- `GET /health`
- the completion-aware flush barrier

Backup is inventoried in Session 0 but remains outside the POC implementation
unless a release-readiness decision pulls it forward.

## Measurement contract

Every comparison uses deterministic OTLP bodies and query sequences and
records:

- offered, admitted, and SQLite-completed spans/s;
- admitted/completed/failed requests and spans;
- queued/in-flight batches, spans, and body bytes; oldest queue age;
- parse, protobuf/gzip decode, batch encode, admission wait, writer queue wait,
  SQLite statement, transaction, and flush time;
- write and query p50/p95/p99 by route and query shape;
- candidate blocks, trace-index rows, payload blocks read, decoded spans,
  matched spans, returned traces/spans, frame bytes, and response bytes;
- read-permit wait/hold time, writer wait/timeouts, cancellation, and retries;
- raw/compressed blocks, logical payload bytes, SQLite file/WAL/SHM bytes,
  freelist/high-water behavior, and bytes/span;
- compaction input/output/rewrite amplification, duration, backlog, prune work,
  and ingest pause;
- Linux process `VmHWM` and final `VmRSS`;
- exact decoded HTTP parity plus storage-oracle parity after flush, optimize,
  reopen, and crash; and
- graceful drain time with final queue/in-flight `0/0`.

The pinned matrix includes:

1. direct extension batch ingest, no queries;
2. current Elixir API/block-store control with zero/one/two query workers;
3. Rust API/libSQL with zero/one/two query workers;
4. every individual read shape on fixed seeded data;
5. maintenance deferred and maintenance under load;
6. one/two/four/eight reader sweep after query fixes; and
7. final drain, graceful restart, kill-9 recovery, file reuse, and HWM.

The Elixir product/block store is the primary process-boundary control because
`timeless_traces` does not currently have an Elixir+libSQL engine. Direct
extension measurements isolate storage capability; they are not substituted
for the HTTP control.

## Session 0 — Pin the real control and compatibility contract

- [x] Add one black-box data-plane fixture for both the current Elixir HTTP
      service and the future Rust server.
- [x] Pin rich OTLP JSON, protobuf, and gzip ingest, including the fields the
      current extension cannot yet preserve.
- [x] Pin decoded Jaeger services, operations, trace lookup, and trace-search
      responses; record intentional ordering normalization separately from
      semantic equality.
- [x] Inventory current OTLP fields and Jaeger parameters as preserved,
      normalized, rejected, or ignored. Do not label the subset “compatible”
      without this table.
- [x] Make `container_http_workload.exs` deterministic and completion-aware.
      Stop reporting a successful HTTP response as durable span throughput.
- [x] Add additive control health/flush counters needed to observe admitted,
      completed, queued, in-flight, rejected, and drained work.
- [x] Capture current Elixir zero/one/two-query baselines with maintenance
      deferred, every query-shape p95/p99, storage size, and HWM.
- [x] Capture direct extension v0 batch ingest and the currently supported
      ten-column query shapes on the same core-span subset.

Artifacts:

- `test/data_plane_contract_test.exs` in `timeless_traces`;
- a deterministic completion-aware workload under `bench/`;
- `bench/results/2026-08-02_traces_api_session0.md`; and
- a checked compatibility inventory in that result.

Exit criterion: satisfied. The current API/storage result is executable and
exact, and no subsequent Rust number can confuse admission, durability, or
lost span fields. Results are recorded in
`bench/results/2026-08-02_traces_api_session0.md`.

## Session 1 — Close libSQL span-fidelity parity

- [x] Extend the public vtab schema additively for status description, events,
      resource attributes, and instrumentation scope. Preserve typed JSON
      values; do not hide them in a server-private blob.
- [x] Decide and document the current product behavior for OTLP links and other
      presently ignored fields before changing it.
- [x] Add a versioned traces batch revision for the rich span shape while
      keeping batch v0 and existing block generations readable.
- [x] Evolve the span codec compatibly. Old blocks return documented defaults;
      new blocks survive optimize and reopen without lossy conversion.
- [x] Derive/index `service.name` consistently from resource/span attributes
      while retaining the explicit public `service` query column.
- [x] Preserve typed attribute/resource/event values through SQL JSON text,
      batch ingest, compression, and Jaeger tag/log encoding.
- [x] Add row-versus-batch, buffer-threshold, transaction/savepoint/rollback,
      flush, optimize, prune, reopen, corruption, and crash regressions.
- [x] Extend the randomized plain-table oracle with rich spans and compare
      semantic JSON values, packed IDs, timestamps, and all query families.
- [x] Measure storage and direct ingest regressions against the existing v0
      core-span fixture. Record an honest size cost for fidelity.

Exit criterion: satisfied. One hundred percent of the Session 0 rich fixture
survives direct SQLite/libSQL ingest and cold reopen, with backward
compatibility and no server code, in `timeless-libsql` commits `368e204` and
`2176321`. Results, including direct v0/v1 storage, ingest, query tails, HWM,
compatibility, and crash evidence, are recorded in
`bench/results/2026-08-02_traces_api_session1.md`.

## Session 2 — Build the descriptive Rust server shell

- [x] Add `poc/timeless-traces-api`; do not create `timeless-api` or share
      metrics/logs route modules prematurely.
- [x] Load the extension, create/connect one traces vtab, and use the same
      vtab arguments/retention contract as direct callers.
- [x] Start one ordered writer and a configurable bounded reader pool, with two
      readers only as a provisional correctness default.
- [x] Add a bounded command queue and exact request/span/body watermarks.
- [x] Add liveness, readiness, stats, and completion-aware flush endpoints.
- [x] Acquire an owner lease before SQLite, reject a second owner, and expose a
      clear capability/version mismatch before accepting traffic.
- [x] Handle `SIGINT`/`SIGTERM` through stop-admission, drain, flush,
      checkpoint, and child exit. Keep kill-9 semantics honest: flushed data is
      durable; the admitted unflushed tail may be lost but must never corrupt.
- [x] Pin oversized-body pre-admission rejection, queue saturation, shutdown,
      restart, and cold reopen with extension-backed tests.

Exit criterion: satisfied. The traces-specific binary owns lifecycle and
durability before OTLP or Jaeger implementation, and every test reaches the
public extension rather than benchmark-only storage. Evidence is recorded in
`bench/results/2026-08-02_traces_api_session2.md`.

## Session 3 — OTLP JSON/protobuf ingest through the public batch

- [x] Implement OTLP JSON, protobuf, and gzip parsing with the pinned Session 0
      validation and response behavior.
- [x] Enforce the 10 MiB limit before admission and a decompressed-size limit
      for gzip so compressed bodies cannot bypass memory bounds.
- [x] Parse one complete request, encode one rich columnar batch, and execute
      one hidden-column insert. Keep the extension's 8,192-span flush intact.
- [x] Return the current `ExportTraceServiceResponse` shape and exact accepted/
      rejected accounting; never silently accept data the writer later loses.
- [x] Prove root/child IDs, all kinds/statuses, typed attributes, events,
      resources, scope, status description, timestamps, and malformed bodies.
- [x] Run direct-extension, Rust HTTP, and Elixir control no-query comparisons,
      including parser/batch/SQLite phase attribution, queue drain, storage,
      and HWM.

Exit criterion: satisfied. All three wire encodings persist the exact rich
fixture after flush/reopen with bounded raw/decompressed bodies, one public
batch statement per request, no per-span SQL, and a `200` that covers SQLite
completion. Results are recorded in
`bench/results/2026-08-02_traces_api_session3.md`.

## Session 4 — Jaeger discovery, trace lookup, and search parity

- [x] Implement services, operations, trace-by-ID, and trace-search routes with
      the pinned Jaeger envelopes and time/duration unit conversions.
- [x] Assemble complete traces across block boundaries with deterministic span
      order and exact parent/resource/process/event/status rendering.
- [x] Define whether `limit` counts traces or spans. Match the established
      product contract first, then document any standards correction as an
      explicit compatibility change.
- [x] Use only public extension surfaces. Add a reusable discovery/trace query
      primitive first if raw vtab rows or shadow-table knowledge would leak
      into the API.
- [x] Add per-request cancellation with a scoped SQLite progress handler and
      host-loop checks; prove the same reader is reusable after cancellation.
- [x] Differential-test every route against the Session 0 fixture before
      timing fixed exact, selective, fan-out, duration, discovery, and full
      trace shapes.

Exit criterion: satisfied. Every declared Jaeger route is socket-to-response
Rust and matches the pinned Session 0 semantic oracle. The established span-
limit-before-grouping behavior remains explicit, malformed duration/negative
limit input is a visible 400, and dropped requests interrupt extension work
and leave the same reader reusable. Results are recorded in
`bench/results/2026-08-02_traces_api_session4.md`.

## Session 5 — Reusable read-path and memory acceleration

Apply changes only when Session 4 counters identify the work:

- [ ] Port the logs stable-snapshot pattern to traces: capture candidate block
      locations and buffer generation under the transition guard, then release
      the guard before safe payload reads, decode, filtering, and JSON work.
- [ ] Teach traces `xBestIndex`/engine queries exact
      `ORDER BY start_ts ASC|DESC LIMIT/OFFSET` intent where SQLite rechecks
      cannot invalidate the bounded prefix.
- [ ] Prevent whole-result materialization in the traces cursor. Stream blocks
      or return a versioned packed frame; reject any design retaining
      database-sized payload/result copies.
- [ ] Add native trace-search, duration/attribute filtering, scalar count, or
      discovery primitives only for measured expensive shapes and expose them
      to direct SQLite/libSQL users.
- [ ] Evaluate per-block duration aggregates for bucket/overview queries only
      if the existing decode-bound `timeless_trace_buckets` path is material.
- [ ] Preserve writer fairness already shared from logs and prove forced
      flush/optimize/prune publication interleavings remain exact.
- [ ] Repeat isolated shapes and mixed one/two-query workloads after each
      change; keep or revert independently.

Exit criterion: query CPU no longer blocks writer progress, row-returning
queries have bounded result memory, scalar queries never construct rowsets,
and mixed HWM is suitable for an embedded service.

## Session 6 — Elixir control-plane seam and process isolation

- [ ] Reuse the proven metrics OTP child lifecycle mechanics, but keep a
      traces-specific HTTP client/data source.
- [ ] Decide after three signals whether the identical executable supervision
      code should become a small shared `TelemetryDataPlane.Process`; do not
      merge signal-specific clients or route semantics.
- [ ] Switch one real `TimelessTracesDashboard` historical search and one
      trace-detail lookup behind an opt-in data-plane source. Keep dashboard
      session/state/rendering in Phoenix.
- [ ] Make a complete response all-or-error; invalid JSON, truncated bodies,
      disconnects, and child restarts never become partial trace waterfalls.
- [ ] Force `SIGKILL`, prove OTP restart without a BEAM/dashboard crash, and
      recover the exact flushed trace. Prove normal OTP shutdown sends
      `SIGTERM`, drains, flushes, reaps the child, and leaves no orphan.
- [ ] Keep live tail, alerts, backup UI, token policy, and cluster state out of
      this POC seam unless a historical-query integration cannot be tested
      without them.

Exit criterion: one real dashboard search/detail path crosses the boundary
with exact results and negligible supervision overhead; both abnormal and
normal lifecycle ownership are proven.

## Session 7 — Scheduling, maintenance, and final verdict

- [ ] Sweep one/two/four/eight readers after functional/query fixes.
- [ ] Run zero/one/two-query workloads, every included read shape,
      maintenance under load, final drain, HWM, and fixed response parity.
- [ ] Measure raw compression and compressed merging separately. Reuse the
      logs size-tiered/budgeted optimize policy only if traces currently shows
      rewrite amplification; do not assume it does.
- [ ] Exercise real expiration, SQLite page high-water/reuse, WAL checkpoint,
      and explicit offline vacuum separately. Do not call logical optimize a
      physical shrink.
- [ ] Add API admission fairness or host transaction grouping only if queue,
      writer-wait, and completion evidence identifies a real problem.
- [ ] Compare final Rust API/libSQL with the current Elixir API/block store and
      direct extension on the same host. Preserve honest cases where the
      existing block engine or a plain index wins.
- [ ] Run the full traces/dashboard suites, Rust workspace/Clippy, extension
      SQL/CLI/oracle/crash suite, rich-span contracts, HTTP fixtures,
      cancellation, owner lease, and child-restart gates.
- [ ] Record keep/reject, measured reader default, declared API subset, and the
      exact next product/release boundary.

Exit criterion: a process-boundary decision based on exact field and wire
compatibility, durable completed work, bounded tails/memory, operational
isolation, and honest physical storage behavior.

## Explicitly deferred from the POC

- authentication/token management and tenant policy;
- TLS termination and public-network exposure;
- backup/restore product workflow;
- remote libSQL replication, cluster membership, routing, and failover;
- live-tail transport across the process boundary;
- a unified metrics/logs/traces daemon or generic route framework;
- changing the default `timeless_traces` storage path;
- automatic vacuum policy; and
- OTLP/Jaeger features the current product does not support, unless Session 0
  classifies one as a correctness requirement rather than product expansion.

## Release readiness beyond authentication

Authentication is necessary for a network-reachable daemon, but it is not the
only remaining release work. Release scope matters.

### Extension improvements

Reusable `timeless-core`/`timeless-libsql` fixes from the logs and metrics POCs
can ship independently of the daemons. They already run the workspace,
SQL/CLI, randomized oracle, transaction/savepoint, and kill-9 crash suites.
They still need the normal version/changelog/artifact release process, but do
not need to wait for a unified API server.

### Opt-in local daemon preview

Before publishing the Rust API binaries as an experimental loopback/Unix-
socket option, complete these gates in addition to auth:

1. **Promote and package the binaries.** Move supported crates out of `poc/`
   or otherwise make them first-class workspace/release artifacts; assign
   versions, lock dependency policy, build Linux/macOS artifacts, and publish
   checksums/SBOM/license notices.
2. **Version the server/extension contract.** Add an explicit capability and
   minimum-version handshake so a newer binary cannot accept traffic against
   an incompatible extension. Define additive database-schema migrations and
   downgrade behavior.
3. **Finish process ownership for every released signal.** Metrics has an OTP
   supervision proof; logs does not yet have the equivalent product seam.
   Standardize readiness, owner fencing, graceful drain, abnormal restart,
   child reaping, and configuration validation.
4. **Declare the supported API surface.** Logs and metrics POCs intentionally
   implement subsets; metrics still lacks full PromQL and product endpoints.
   A preview may expose a documented subset, but every unsupported route must
   be explicit and must not silently fall back across the process boundary.
5. **Provide backup/restore and upgrade drills.** Define a consistent SQLite/
   libSQL backup, restore verification, WAL/checkpoint behavior, and recovery
   from an interrupted upgrade. For existing users, document whether preview
   starts with a fresh database or provides an importer.
6. **Run production fault and soak gates.** Sustained mixed load across
   maintenance intervals; slow/disconnected clients; cancellation storms;
   disk-full/read-only filesystems; WAL growth; corrupt blocks; port/address
   conflicts; repeated crash/restart; file-descriptor pressure; and bounded
   RSS over hours, not only short ramps.
7. **Expose operational observability.** Separate liveness/readiness, stable
   machine-readable stats, structured logs, queue/maintenance/error counters,
   build/version identity, and actionable failure messages. Define initial
   SLOs and alert thresholds.
8. **Choose the local transport/security envelope.** Prefer loopback or Unix
   sockets by default. If binding beyond loopback, add auth, secret rotation,
   request identity/audit, rate limits, and a documented TLS termination model
   before calling it supported.
9. **Document resource and data limits.** Maximum body, decompressed body,
   query resolution/rows/cardinality, concurrent reads, queue bytes, disk
   retention, and response size must fail predictably rather than depend on
   host memory.
10. **Exercise installation and clean removal.** Phoenix/Stack configuration,
    executable/extension discovery, writable paths, service startup order,
    upgrades, rollback, and uninstall must work without hand-editing build
    paths.

With those gates, a single-node, single-tenant, loopback-only **experimental
preview** is reasonable even if the API subset is intentionally narrow.

### Default production replacement

Making the Rust data plane the default requires additional product decisions:

- migration/import from existing metrics, logs, and traces data stores;
- full required Prometheus/Victoria/Jaeger/OTLP compatibility or a versioned
  compatibility statement accepted as the product contract;
- cluster ownership, routing, fencing, failover, replication, and backup
  coordination administered by Phoenix;
- tenant isolation and quotas if one daemon/database serves more than one
  application or organization;
- rolling upgrade and mixed-version behavior; and
- a common packaging/configuration story across all three signal processes.

The practical release sequence is therefore:

1. ship reusable extension improvements;
2. finish the traces POC and extract supported daemon crates;
3. release an opt-in local single-node preview after the preview gates;
4. add auth/token management and the remaining operational/product work; and
5. change defaults only after migration and cluster behavior are proven.

## Tomorrow's starting point

Begin with Session 0 only. Do not create `timeless-traces-api` until the rich
OTLP fixture, current durable baseline, and field-loss matrix are checked in.
The first implementation session after that is extension fidelity, not HTTP.
