# Changelog

This changelog starts at 1.4.5; earlier releases are recorded by git
tags and `bench/results/*.md` session documents.

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
