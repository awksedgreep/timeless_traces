# Traces API POC Session 6 results

Date: 2026-08-02

Session 6 connects one real TimelessTracesDashboard historical search and one
trace-detail waterfall to the standalone Rust process without moving Phoenix
session, state, rendering, statistics, or live tail across the boundary.

## Baseline and branches

Before implementation, `timeless_traces_dashboard` `main` at `b4a445a` was
clean and passed its four tests in 0.02 seconds. It had no POC branch, so
`poc/rust-telemetry-data-plane` was created from that commit rather than
modifying `main`.

The successful session is committed and pushed directly, with no PR or merge:

- `timeless-libsql` `28897a4` — lossless native dashboard query routes;
- `timeless_traces_dashboard` `dba4f99` — OTP owner, complete-response client,
  opt-in historical source, real Page seam, fault gates, and boundary bench.

The existing POC database and extension contract did not change. Every query
still reaches `CREATE VIRTUAL TABLE traces USING timeless_traces`; the API has
no storage implementation or host span buffer, and the extension's fixed
8,192-span batching remains authoritative.

## Fidelity boundary discovered and closed

Using the Jaeger envelope as a dashboard transport would have been lossy even
though that envelope remains semantically correct for Jaeger. Jaeger processes
can merge resource data by service and have no native representation for the
complete per-span instrumentation scope. Reconstructing
`TimelessTraces.Span` from that response would therefore violate the rich-span
contract.

The regression is closed with two traces-specific, lossless routes over public
vtab rows:

- `GET /select/timeless/api/spans` implements the dashboard's bounded
  historical pagination, ordering, name/string-attribute search, service,
  kind, status, and time filters;
- `GET /select/timeless/api/traces/:trace_id` returns the complete native trace
  in deterministic start/span order.

Both carry packed IDs as canonical hex plus nanosecond start/end/duration,
kind/status, nullable status description, typed attributes, typed events,
per-span resource, and instrumentation scope. Jaeger routes and semantics are
unchanged. The name filter stays exact to the current product: a
case-insensitive substring of the name or any string-valued span attribute.
Its host loop consumes the streaming vtab cursor and stops after
`OFFSET + LIMIT + 1` matches instead of materializing the database.

## Elixir seam and all-or-error contract

`TimelessTracesDashboard.HistoricalSource` defaults to the existing local
`TimelessTraces` calls. The opt-in `HistoricalSource.DataPlane` switches only
Page search and trace detail. Stats and live tail still take their established
Elixir paths.

The traces-specific client accepts only IPv4 loopback HTTP, waits for the
supervised child's readiness, receives one complete bounded response, decodes
it once, and validates every span before returning any result. It rejects:

- invalid or truncated JSON;
- a connection closed before the declared body completes;
- a malformed later span after earlier valid spans;
- IDs, timestamps, duration consistency, kind/status, or rich-field shape that
  cannot produce an exact `TimelessTraces.Span`; and
- a trace response containing an ID other than the requested trace.

Thus a disconnect or restart is an operation error, never a partial LiveView
waterfall.

## Lifecycle proof

The OTP port owner reuses the proven metrics mechanics with traces-specific
names and readiness:

1. start one exclusive `timeless-traces-api` child and wait for its listener;
2. ingest the two-span rich fixture and cross a completion-aware flush barrier;
3. render both a real Page search and Page trace detail with exact typed fields;
4. send `SIGKILL` to the OS child and observe the owner exit;
5. let the OTP supervisor restart it and retrieve the exact flushed trace;
6. admit a new one-span rich tail while scheduled flush is deferred;
7. stop the OTP child normally, which sends `SIGTERM`, drains, flushes, and
   reaps the OS child; and
8. restart and retrieve that formerly unflushed tail exactly, then stop and
   prove the final OS PID no longer exists.

The BEAM, test process, and dashboard remain alive across the abnormal exit.
The exact restart result proves the flushed prefix; the exact newly admitted
tail after normal stop proves the graceful final flush rather than merely a
child exit.

## Boundary measurement

`timeless_traces_dashboard/bench/traces_data_plane_boundary.exs` ran five
paired rounds of 500 direct-endpoint and supervised-process lookups per round
(2,500 samples per path) against the release binary and extension. Each result
contained the exact two-span rich trace.

| measurement | result |
|---|---:|
| startup to ready | 5,896 us |
| OTP owner memory | 109,304 bytes |
| Rust `VmHWM` | 9,968 KiB |
| direct endpoint p50 / p95 / p99 | 384 / 645 / 724 us |
| supervised endpoint p50 / p95 / p99 | 382 / 657 / 715 us |
| supervision lookup p95 delta | 12 us |
| `SIGKILL` to restarted ready | 20,089 us |

The fixture's completion/storage snapshot before the restart was:

| measurement | result |
|---|---:|
| admitted / completed spans | 2 / 2 |
| queued / in-flight spans | 0 / 0 |
| logical compressed payload | 835 bytes |
| SQLite index pages | 65,536 bytes |
| database / WAL / SHM | 16,384 / 180,520 / 32,768 bytes |

The 12 us p95 lookup delta, 107 KiB owner process, and 5.9 ms startup make the
supervision seam negligible relative to even this sub-millisecond local trace
lookup. Session 7 retains responsibility for the fixed large-data read/write,
maintenance, physical-storage, and whole-process HWM verdict.

## Shared lifecycle decision

The three signal POCs now justify a small shared executable-owner abstraction,
but not shared clients or routes. Extraction is accepted for the artifact
promotion/release boundary, into a neutral dependency containing only:

- executable/extension/database/listener validation;
- OTP port ownership and readiness parsing;
- loopback endpoint and OS PID inspection; and
- `SIGTERM` drain timeout, `SIGKILL` fallback, restart, and reaping.

It is deliberately not extracted into `timeless_ui` or a signal package during
this POC: making the traces dashboard depend on the metrics UI would invert
ownership, while creating a new public package before the POC binaries are
promoted would manufacture a release surface prematurely. Signal-specific
clients, response validation, routes, and semantics remain separate.

## Verification

- dashboard format gate and full suite: 11 passed;
- real release-binary lifecycle/integration gate: passed;
- Rust traces API: 8 library, 1 binary, 4 Jaeger, 4 OTLP/dashboard, and 4
  storage tests passed;
- Rust traces API Clippy all targets with `-D warnings`: passed;
- lossless native route, invalid limit, malformed later span, truncated body,
  disconnect, trace-ID mismatch, real Page seam, `SIGKILL` restart, graceful
  tail flush, and orphan-reaping regressions: passed.

Session 6 exit criterion is satisfied. Session 7 can now measure scheduling,
maintenance, final tails/storage/HWM, and make the process-boundary verdict.
