# Performance Audit Plan — Shared-Defect Findings from timeless_logs

**Date:** 2026-07-18
**Status:** findings recorded, fixes NOT yet applied. This is the work
plan for the timeless_traces performance branch.

timeless_logs 1.5.0 just went through a full performance overhaul
(branch `perf/read-path-and-backpressure`; see
`timeless_logs/bench/results/2026-07-18_perf_audit_i185.md` and
`.../2026-07-18_read_path_backpressure_validation.md` for the audit,
the fixes, and the validated numbers: durable ingest 227K → 388K
entries/s, query p50 seconds → 3-17ms under load, RSS 32GB → bounded).

timeless_traces (1.3.16) is a copy-and-adapt sibling of timeless_logs
and was scanned for the same defects on 2026-07-18. Verdicts below with
evidence; the fix for each is a port of the corresponding timeless_logs
commit on that branch.

## Shares the defect — apply the playbook

1. **`count_total: true` default forces full block decompression**
   (index.ex:932,935). Worse than logs: the `overlapping_blocks?` check
   (index.ex:1134-1158) can escalate collection to `:all`. Port: default
   count_total off on the HTTP query path; per-term entry counts in the
   term index (schema migration) for cheap exact totals; honest
   early-exit. Logs saw 22x on limit queries, 570x on counts.

2. **Compactor reads the ENTIRE raw backlog into memory per pass**
   (compactor.ex:90-106) — OOM hazard once it falls behind — then sorts
   it, and compresses **sequentially** (no Task.async_stream; logs at
   least parallelized). Fixed interval + idle backoff only. Port:
   bounded passes (entry budget), continuous rescheduling under debt,
   parallel chunk compression, adaptive level under pressure.

3. **No drain-coupled ingest backpressure.** Bounded in-flight flushes
   exist (buffer.ex:8,129-132) but the pending `:queue` and the shard +
   Index mailboxes are unbounded — the exact holes found in logs
   (32GB RSS under overload there). Port the final gauge design and its
   three hard-won lessons: producer-side atomics increment BEFORE send,
   credit only when the block row is durable in SQLite, and producers
   sleep-poll the gauge when over watermark (waiting on a GenServer call
   reply is NOT backpressure — the shard hands work downstream faster
   than it persists). Include compactor raw-debt in the overload signal.

4. **Query decompression fans out at full scheduler width**
   (index.ex:1063,1101,1182), uncoordinated with flush and compaction.
   Port: query_concurrency cap (cores/2).

5. **No hot tail: spans invisible to queries until flush + index**
   (index.ex:37-47 — subscribers get Registry broadcasts, queries see
   only indexed blocks). For traces this is a UX papercut ("where is my
   span?"). Port: ETS ordered_set fed at accept, boundary-partitioned
   queries (tail serves ts >= boundary, disk serves older — exact union,
   no dedup). CRITICAL porting lesson: tail reads must be bounded
   (prev/next walks for pages, select-continuation chunks for counts,
   select_delete pruning) — the unbounded first version in logs made
   everything WORSE (12s p50, 9GB RSS).

6. **Retention is pressure-unaware** (retention.ex:39-67): size-based
   deletion can treadmill against a flushing backlog. Port: skip
   size/term cleanup while ingest is overloaded; purge the hot tail
   range on deletion once the tail exists.

7. **Term extraction is per-span with no memoization**
   (index.ex:324-361 `extract_span_terms`). No regexes (cheaper than
   logs was), but `service.name`, `http.method`, `db.system`, `kind`,
   `status` values repeat massively within a block. Port the
   memoize-per-distinct-pair batch extraction — in logs this moved the
   durable wall +71% on its own. Also port per-term entry counts while
   touching this (feeds fix 1).

8. **`Registry.count_match` in the ingest hot path** (buffer.ex:237) —
   already per-batch (better than logs was), verify it stays hoisted
   when porting.

## Does NOT share (verified safe)

- **Atom exhaustion:** uses `String.to_existing_atom` with fallback
  (span.ex:98); attribute keys stay strings. No fix needed.
- **Timestamp units:** nanoseconds throughout (exporter, OTLP ingest,
  filters). No mixed-unit bug. Worth adding the magnitude-normalization
  guard at ingest boundaries anyway (inbound units are trusted, not
  validated), but low priority.

## Traces-specific considerations for the port

- Spans shard by trace_id/service.name (4 shards default — scale with
  cores like logs: cores/2 clamped 4..8).
- The trace_index table (trace_id → block_id) and duration-range queries
  are extra read paths logs doesn't have — the concurrency cap and
  count-fastpath work must cover trace-lookup fan-out (index.ex:1182)
  too.
- Hot tail boundary partitioning must use span start_time (ns) and play
  nicely with trace-assembly queries (a trace's spans may straddle the
  boundary — union is still exact, but trace lookup should query both
  sides by trace_id; tail needs a by-trace_id access path or trace
  lookups accept tail scan).
- Sequencing: build the OTLP-span workload harness first (adapt
  `timeless_logs/bench/container_http_workload.exs`), capture the
  BEFORE numbers, then fix in the same order as logs (read path →
  backpressure → compactor → concurrency → hot tail → batch
  extraction), validating each against the harness.

## Suggested branch name

`perf/read-path-and-backpressure` (mirror of the logs branch).
