# Perf Port Validation (i185)

**Date:** 2026-07-19 (same session as the baseline)
**Machine:** Intel Core Ultra 9 185H ("i185"), Linux, 22 schedulers
**Branch:** perf/read-path-and-backpressure — the timeless_logs 1.5.0
playbook ported per `docs/2026-07-18_perf_audit_plan.md`.
**Method:** identical workload to `2026-07-19_baseline_i185.md`.

## Before / after

| Ingest | Query p50 (baseline) | Query p50 (port) | Q/s (port) |
|-------:|---------------------:|-----------------:|-----------:|
| 8K spans/s | 96ms | **2.3ms** | 182 |
| 32K | 1.25s | **2.5ms** | 182 |
| 62K | 1.86s | **3.8ms** | 154 |
| 123K | 5.96s | **9.9ms** | 123 |
| 208K (wall) | zero completions | **21.8ms** | 114 |

- **Durable wall: 208K spans/s, honest** (write p99 109ms = producers
  pacing). The baseline's 467K/s was an accept rate hiding ~9.5M spans
  in RAM; its true durable rate was far lower and degrading.
- **RSS bounded: 1.9GB** at the wall with a 2.5GB dataset (roughly half
  is SQLite mmap), vs 6.7GB unbounded at baseline. Zero backlog at
  bench end: ~6.75M spans sent == 6.66M on disk + watermark.
- Midpoint measurement (read path + backpressure + compactor + caps,
  before the hot tail): wall 116K spans/s, query p50 10-138ms. The hot
  tail then both cut query p50 to single-digit ms AND lifted the wall
  to 208K — queries stopped competing with the flush pipeline for disk.

## What was ported (see the plan doc for the defect list)

1. Read path: count_total off on the Jaeger HTTP search; per-term
   entry counts (schema v2); **waterline early-exit** — traces-specific
   design: blocks ordered by contribution bound (ts_max DESC for :desc),
   merge into a running top-(need+1), halt when no remaining block can
   intrude on the page. Replaces the overlapping-blocks full-collection
   escalation with correct O(page) pagination.
2. Drain-coupled backpressure (producer-side gauge, credited at SQLite
   durability, sleep-poll above watermark).
3. Compactor: bounded continuous passes, chunked PARALLEL compression
   (was fully sequential single-output), adaptive level under debt,
   raw-debt gauge feed, multi-output compact_blocks.
4. Query concurrency capped at cores/2 (search, trace lookup).
5. Hot tail with a trace_id side index (duplicate_bag trace_id -> key):
   spans queryable at accept; trace-by-id lookups never scan the tail;
   trace assembly merges tail + disk with span_id dedup (spans straddle
   the boundary).
6. Retention: pressure-skip for size/term cleanup; tail purge on delete.

## Bonus correctness find

Age-based retention computed a SECONDS cutoff against NANOSECOND block
bounds — `ts_max < cutoff` was never true, so age retention has never
deleted a traces block. Fixed (same class of bug as timeless_logs'
mixed-unit timestamps).

## Not ported (documented follow-up)

Memoized batch term extraction (logs' +71% wall lever). Traces term
extraction has no regexes (hardcoded prefix terms), so the expected win
is smaller; per-term counts already landed with the schema change. If
the wall needs to move past 208K, decompose the drain per-stage first
(the logs method) — likely suspects are OTLP JSON parsing (nested
attribute arrays, much heavier than NDJSON) and per-span term string
building.
