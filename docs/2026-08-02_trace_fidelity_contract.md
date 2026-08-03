# Rust traces storage fidelity contract

The release stores the rich span model exposed by TimelessTraces. This is a
contract for the fields the product persists, not a claim that every OTLP wire
field is retained.

## Preserved exactly

- trace ID, span ID, and parent span ID (including root-vs-child identity);
- name, kind, start/end timestamps, duration, status, and status message;
- typed span attributes;
- events, event timestamps, and typed event attributes;
- typed resource attributes;
- instrumentation scope name and version; and
- service identity derived from resource/span attributes while retaining the
  explicit service query column.

These fields are covered by the rich JSON/protobuf ingest, flush, optimize,
reopen, migration, and Jaeger contract tests. Conversion must copy them; it
must never replace an unavailable value with an empty string, zero, or empty
map.

## Not part of the stored model

The current TimelessTraces schema does not retain OTLP links, tracestate,
trace flags, remote-parent state, schema URLs, or dropped-count metadata.
Those fields are not represented in query or migration results. The release
therefore documents them as unsupported storage features; their absence is not
reported as successful preservation and no values are synthesized during
conversion.

Adding any of these fields is a separate additive schema/API change requiring
new ingest, cold-reopen, query, Jaeger/OTLP, backup, migration, and rollback
regressions before it can be advertised.

## Compatibility rule

Legacy rows are immutable conversion input. A conversion that cannot validate
the preserved fields fails closed and leaves the legacy store usable by the
previous release. Post-cutover semantic queries must match the supported field
inventory above exactly.
