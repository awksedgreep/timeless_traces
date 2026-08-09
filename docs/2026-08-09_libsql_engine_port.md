# libSQL engine port (2026-08-09)

Status: **engine surface complete, opt-in.** The traces instance of the
port pattern proven in timeless_metrics (shipped 6.3.0) and timeless_logs
(`timeless_logs/notes/libsql_engine_port_plan_2026-08-09.md`) — see that
plan for the full rationale: the embedded Elixir block engine is
deprecated; the in-process libSQL engine keeps the dashboard/canvas
latency budget and gives embedded and external (Rust
`timeless-traces-api`) modes ONE on-disk format, so hosts graduate by
switching owners, not migrating data.

What landed:

- `TimelessTraces.LibsqlEngine` (opt-in via
  `config :timeless_traces, engine: :libsql`): writer over
  `<data_dir>/traces.db` with the migration candidate's validated
  rich-span-v1 batch encoder, capability preflight, flush/optimize
  command channel, trap-exit + terminate flush, unmigrated-legacy-store
  refusal.
- Query surface: facade filters with service/kind/status/trace_id/
  start-range/duration-range pushed into the vtab scan and the SHARED
  `TimelessTraces.Filter` applied as residual — parity by construction.
  `trace/1` accepts raw 16-byte and 32-hex ids. `services/0`,
  `operations/1` via DISTINCT; `stats/0` from `timeless_stats('traces')`;
  `backup/1` as a single VACUUM INTO snapshot.
- `TimelessTraces.StorageEngine` seam routes the facade, the OTel
  exporter, and HTTP ingest; subscriber broadcast happens in the seam on
  the libSQL path so `subscribe/1` sees one stream on either engine.
- Default remains `:elixir` (unchanged); the flip ships as its own
  release per policy.

Remaining (mirrors the logs plan): perf/soak gate, default-flip release,
legacy-engine retirement on the deprecation schedule. Rich-span-v2
fields (links, trace_state, flags, schema URLs, dropped counts) stay
explicitly out per `2026-08-02_trace_fidelity_contract.md`.
