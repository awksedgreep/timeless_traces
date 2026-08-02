# Rust traces API POC — Session 3

Date: 2026-08-02  
Branch: `poc/rust-telemetry-data-plane`

## Verdict

Session 3 passes. OTLP JSON, protobuf, and gzip-compressed protobuf enter the
same traces-specific Rust route, preserve the exact Session 0 rich fixture,
encode once into public rich batch version `0x02`, execute one hidden-column
insert per HTTP request, and survive explicit flush plus cold reopen.

The exact Session 0 zero-query ramp reached **265.5K durably completed
spans/s**, 16.2% above the Elixir/block-store control's 228.5K peak. At its
highest non-saturated offered step Rust completed 240.6K spans/s with a 31.78
ms write p99; the comparable Elixir peak had a 70.79 ms p99. Rust's process HWM
was 377,772 KiB after 7.53M spans, versus 811,812 KiB after 7.08M spans for the
Elixir control: 53.5% lower at a slightly larger final dataset.

This is completed work, not accepted-response throughput. Every `200` waits
for its request's single SQLite statement. The final flush response covered
all 15,051 admitted/completed requests and 7,525,500 spans, with failed,
queued, and in-flight requests/spans all zero.

## Wire and validation contract

- The response remains exactly `{"partialSuccess":{}}` with status 200.
- OTLP JSON numeric and named kind/status values map to the five/three storage
  enums; unknown values retain the established internal/unset defaults.
- JSON string-encoded int64 attributes remain strings, matching the Session 0
  inventory. Primitive types are preserved. Standard array and key-value-list
  AnyValues are now preserved structurally instead of adding further loss.
- Protobuf byte attributes retain the established base64 representation.
- Trace/span/parent IDs, signed-64-bit nanosecond bounds, and non-negative
  computed duration are validated before admission. One invalid span rejects
  the whole request; a valid sibling is never partially stored.
- Empty `resourceSpans` is an accepted zero-span request. Missing
  `resourceSpans`, invalid JSON/protobuf/gzip, invalid IDs/times, and malformed
  shapes return 400 before admission.
- As in the current Elixir route, any content type not containing
  `application/x-protobuf` follows the JSON path.
- Both the compressed/raw request and decompressed gzip payload are capped at
  10 MiB. A compressed expansion beyond the cap returns 413 before parsing or
  admission.
- Links, trace state/flags, schema URLs, scope attributes, and dropped counts
  retain the explicit Session 0 omissions; the server does not claim full OTLP
  conformance.

## Exact Session 0 zero-query ramp

Same host, deterministic generator, seed `20260802`, 16 writers, 500
spans/request, 10-second steps, 3-second warmup, maintenance deferred, and
retention disabled:

| interval | Rust durable spans/s | Rust p99 | Rust HWM KiB | Elixir durable spans/s | Elixir p99 | Elixir HWM KiB |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 ms | 8.0K | 12.37 ms | 55,828 | 8.0K | 4.78 ms | 345,996 |
| 500 ms | 15.9K | 14.11 ms | 91,748 | 15.9K | 5.69 ms | 517,640 |
| 250 ms | 31.7K | 13.76 ms | 172,540 | 31.6K | 5.86 ms | 600,288 |
| 125 ms | 63.0K | 14.03 ms | 196,008 | 62.9K | 5.87 ms | 600,288 |
| 62.5 ms | 124.6K | 22.12 ms | 234,204 | 124.0K | 9.59 ms | 636,496 |
| 31.25 ms | 240.6K | 31.78 ms | 289,732 | **228.5K** | 70.79 ms | 811,812 |
| 15.63 ms | **265.5K** | 49.26 ms | 377,772 | 227.2K | 100.82 ms | 1,614,968 |

The final Rust step was correctly classified saturated because 265.5K was
below 60% of the 512K offered target, not because of errors or p99. The Elixir
control saturated at the same step on its 100 ms p99 gate.

## Phase attribution

A separate fixed 800,000-span, 16-writer, 500-span/request run completed
800,000/800,000 with zero failures at 183.7K spans/s and 260,036 KiB HWM. Its
server counters (sums across concurrent requests) were:

| phase | total | per request |
|---|---:|---:|
| JSON decode + normalize | 3.859 s | 2.412 ms |
| rich batch encode | 68.5 ms | 0.043 ms |
| SQLite statement/autocommit transaction | 1.697 s | 1.060 ms |
| admission wait | 0.805 ms | 0.0005 ms |
| writer queue wait | 18.116 s | 11.323 ms |
| final extension flush | 0.645 ms | — |

Queue wait is accumulated across 1,600 concurrent requests and is therefore
larger than wall time. The phase counters identify JSON normalization and the
single ordered writer as future tuning surfaces; there is no hidden per-span
SQL or server-side block work to remove.

## Storage accounting

The 7,525,500-span exact ramp ended with 889 raw blocks:

| state | logical block payload | SQLite file | freelist | interpretation |
|---|---:|---:|---:|---|
| drained raw | 1,600,222,072 B | 1,642,315,776 B | 0 | ingest result |
| optimized + checkpoint | 184,195,642 B (24.48 B/span) | 1,871,560,704 B | 1,604,288,512 B | logical shrink; physical high-water retained |
| offline vacuum | same logical payload | 225,804,288 B (30.01 B/span) | 0 | explicit physical shrink |

Optimize took 24.97 s. Offline vacuum took 0.48 s. These are reported
separately: optimize is not mislabeled as physical shrink. Before optimize the
Rust raw logical payload was about half the Elixir control's 3,172,025,833
block bytes at a comparable final span count; fixture shape and storage engine
differ, so that is directional rather than a codec-only comparison.

The direct rich-v1 extension control from Session 1 remains the storage ceiling
at 0.38M spans/s. Rust HTTP reaches 69.9% of that direct rate while adding JSON
parsing, HTTP scheduling, exact validation, completion acknowledgement, and
watermarks. Direct and HTTP both retain the extension's 8,192 threshold.

## Regressions and verification

New tests cover:

- semantic JSON/protobuf/gzip equality for root/child IDs, all rich fields,
  service precedence, events, resource, scope, status description, and typed
  values after flush/reopen;
- all five kinds and three statuses in parser mappings;
- malformed wire data, missing fields, invalid IDs/timestamps, a mixed
  valid/invalid request, wrong content type, empty request, raw oversize,
  decompression failure, and compressed expansion oversize;
- exact admitted/completed/rejected request/span/body counters;
- 8,191 spans still buffered after one HTTP request and the next span causing
  the extension's automatic 8,192-span flush; and
- public `disk_spans`/`total_spans` metadata counters for direct SQLite users,
  avoiding a payload-decoding `COUNT(*)` in health checks.

Verification passed:

```text
cargo test --workspace
  171 Rust tests plus doc tests passed

bash tests/cli.sh
  all 43 sections passed
  3 randomized 50K-operation oracle seeds passed
  5 kill-9 crash rounds passed with rich typed JSON

cd poc/timeless-traces-api
cargo clippy --all-targets -- -D warnings
  passed
TIMELESS_EXT_PATH=../../target/debug/libtimeless_ext.so cargo test
  8 unit tests + 7 extension-backed contract tests passed
```

## Next

Session 4 adds the four pinned Jaeger read routes, complete trace assembly,
cancellation, and differential response tests. No read benchmark begins until
those responses are exact.
