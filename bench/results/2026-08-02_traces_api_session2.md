# Rust traces API POC — Session 2

Date: 2026-08-02  
Branch: `poc/rust-telemetry-data-plane`

## Verdict

Session 2 passes its exit criterion. `poc/timeless-traces-api` is a
traces-specific lifecycle shell over the existing public `timeless_traces`
vtab. It contains no server-owned span buffer, block codec, index, shadow
schema, or benchmark storage. OTLP and Jaeger remain visibly unimplemented for
Sessions 3 and 4.

The extension remains authoritative for the 8,192-span automatic flush,
buffered visibility, block creation, compression, trace/term indexes,
retention, transactions, and recovery. The only test ingestion seam accepts
one already encoded public rich batch and executes one hidden-column insert;
it exists for Session 2 lifecycle tests and becomes the Session 3 OTLP writer
seam.

## Implemented boundary

- one SQLite writer consuming a bounded ordered queue;
- a configurable bounded reader pool, provisionally defaulting to two;
- atomic admitted/completed/failed and queued/in-flight request, span, and
  body-byte watermarks, including oldest queue age;
- process-only `/live`, storage-backed `/ready` and `/health`, detailed
  `/select/traces/stats`, and completion-aware `POST /api/v1/flush`;
- the 10 MiB body gate on the reserved OTLP route, which returns `501` for an
  in-range body and `413` before admission for an oversized body;
- an exclusive sidecar owner lease acquired before SQLite is opened;
- startup negotiation of the exact 14 rich columns plus hidden command,
  `timeless_traces` module identity, persisted retention, and public rich batch
  version `0x02` before the listener binds;
- periodic public `flush` and `optimize` commands without a host block policy;
  and
- `SIGINT`/`SIGTERM` HTTP drain, ordered writer drain, public extension flush,
  WAL truncate checkpoint, worker join, lease release, and process exit.

Kill-9 behavior is stated narrowly and tested: the already flushed prefix is
exact and recoverable and the database is not corrupt. The process cannot
promise an admitted tail still resident in the extension buffer when SIGKILL
prevents cleanup; Session 3 will exercise that distinction through HTTP once
OTLP exists.

## Extension-backed regressions

The contract suite uses the actual built `libtimeless_ext.so` and covers:

1. exclusive ownership and lease recovery;
2. rejection of an ordinary/incompatible `traces` table before readiness;
3. rejection of persisted-retention drift on reopen;
4. liveness, readiness, module/capability identity, and stats;
5. an over-10-MiB request rejected before storage admission;
6. one real rich batch, exact request/span/body completion watermarks, ordered
   flush, all rich fields after cold reopen, and zero WAL bytes after graceful
   checkpoint;
7. physical SQLite write-lock saturation with queue capacity one: one request
   in flight, exactly one queued, and a third backpressured without changing
   admission counters, followed by exact `8,193`-span drain; and
8. binary SIGKILL/restart and SIGTERM/restart with the exact flushed rich span
   intact.

The capability test initially rejected a stale top-level debug shared library
that still exposed the old ten-column schema. Rebuilding the loadable artifact
made the contract pass. This is useful evidence that startup will fail clearly
instead of silently dropping rich fields when binary and extension artifacts
are mismatched.

## Verification

From `timeless-libsql`:

```text
cargo build -p timeless-ext
  passed

cd poc/timeless-traces-api
cargo clippy --all-targets -- -D warnings
  passed

TIMELESS_EXT_PATH=../../target/debug/libtimeless_ext.so cargo test
  5 unit tests passed
  4 extension-backed lifecycle/process tests passed
  0 failed
```

The extension-backed suite completes in roughly `0.13 s` once built. Session 2
does not publish ingest/query throughput: doing so before OTLP and Jaeger would
measure a test seam rather than the API boundary. Session 3 starts from the
already recorded direct-extension and Elixir controls.

## Next

Session 3 replaces only the reserved OTLP handler: exact JSON/protobuf/gzip
validation, one parse and one rich v1 batch encode per request, decompressed
body bounds, exact response semantics, fidelity regressions, and the first
honest Rust HTTP ingest comparison.
