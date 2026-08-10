# Changelog

This changelog starts at 1.4.5; earlier releases are recorded by git
tags and `bench/results/*.md` session documents.
## 1.8.0 (2026-08-09)

**The libSQL engine reports trace and span ids as lowercase hex.** The store
keeps them as BLOBs — 16 bytes for a trace, 8 for a span — and the read path
handed the raw blob to callers. Dashboards rendered binary, and a lookup using
an id the engine had just reported would miss. The Elixir engine always
round-tripped hex, and `Index.trace/1` decodes with `Base.decode16!/2`, so hex
is the contract both sides of this library already assumed.

The vtab accepts a blob or hex text on input, so both forms now converge on hex
output. Widths are fixed and distinct, so a value already stored as hex text is
passed through rather than double-encoded.

Minor rather than patch: code written against the raw blob will see a different
value. Two of this library's own tests were in that position — they wrote binary
ids and asserted binary back, encoding the defect.

## 1.7.0 (2026-08-09)

**A block written by an unreadable compression format is no longer reported
as corruption.** `Writer.decompress_block/2` can now answer
`{:error, :incompatible_format}`, and the legacy migration maps that to
`state: :incompatible_version` instead of `state: :corruption`. The two imply
opposite operator responses — corruption means restore from backup, an
unreadable format means the bytes are intact and need a different decoder —
so the old answer sent recovery in the wrong direction.

Previously the decode failure was rescued, logged, and collapsed to a bare
`:corrupt_block`, so nothing downstream could tell the cases apart even in
principle. Classification now uses an allowlist of OpenZL graph/transform
topology signatures, which is what a frame from an older OpenZL looks like to
a newer decoder. Genuine damage announces itself differently, because OpenZL
checksums its payload: overwritten bytes report a checksum mismatch and
truncation reports a short source. Anything unrecognised keeps the historical
`:corrupt_block` answer.

Consumers matching exhaustively on `{:error, :corrupt_block}` should add the
new value; this is why the release is a minor bump.

Adds `tools/legacy_ozl_transcode`, a standalone one-shot utility that rewrites
blocks written by `ex_openzl <= 0.4.6` into the version-independent `:raw`
format. `ex_openzl 0.4.7` moved its vendored OpenZL to v0.2.0, whose decoder
rejects 0.1.x frames, which blocks the legacy-to-libSQL migration for any
store that is not actively rewriting its blocks. The tool is not part of the
package.

## 1.6.0 (2026-08-09)

**Automatic legacy conversion.** Starting on `engine: :libsql` over an
unmigrated legacy block store now runs the journaled, resumable,
digest-verified `ReleaseStartup.prepare/2` conversion automatically at
startup (exclusive owner lock; source retained for rollback), instead
of refusing. Set `auto_migrate: false` to restore the strict refusal.
The legacy Elixir block engine is deprecated for removal in roughly
three months (~2026-11).

## 1.5.0 (2026-08-09)

**Opt-in libSQL storage engine** (`config :timeless_traces, engine:
:libsql`) — the port of the runtime to the timeless-libsql v0.5.0
traces virtual table, replacing the deprecated Elixir block engine for
hosts that opt in. One `traces.db` holds everything; embedded and
external (Rust `timeless-traces-api`) modes share one on-disk format,
so a host graduates to the Rust owner by switching owners, not
migrating data.

- Full facade coverage: ingest (rich-span-v1 batches, OTel exporter and
  HTTP OTLP routed through the engine seam), flush/optimize,
  query (service/kind/status/trace_id/time/duration pushdown + the
  shared Filter residuals — parity by construction), trace lookup by
  raw or hex id, service/operation discovery, stats, VACUUM INTO
  backup, subscriptions.
- Startup refuses an unmigrated legacy block store loudly (run
  `TimelessTraces.ReleaseMigration` first); cold-reopen durability via
  a final flush on shutdown.
- Default engine remains `:elixir`, completely unchanged; the flip
  ships as its own release. Rich-span-v2 fields stay out per the
  fidelity contract.
- Port doc: `docs/2026-08-09_libsql_engine_port.md`.

## 1.4.5 (2026-08-08)

Validated against the released **timeless-libsql v0.5.0** and re-pinned
the CI/release workflows to that tag (they previously built a pre-0.4.0
development rev, 408 commits behind). The capability preflight —
data ABI 1 + rich-span-v1 batches — passes unchanged, and the full
suite (201 tests, including the 8,192-span checkpoint crash-boundary
migration walk and cold-parity validation) is green against v0.5.0.

- The libSQL migration candidate now traps exits so its connection and
  WAL close even when the linked migration caller dies mid-run.
- Note for migrated `traces.db` files: the migration's final `optimize`
  under a v0.5.0 extension writes current-generation blocks; readers of
  that database need timeless-libsql ≥ 0.4.0 (the capability handshake
  line), as before. Span batches remain rich-span-v1, which v0.5.0
  continues to accept; rich-span-v2 fields (links, trace_state, flags,
  schema URLs, dropped counts) stay explicitly unsupported per the
  fidelity contract.
