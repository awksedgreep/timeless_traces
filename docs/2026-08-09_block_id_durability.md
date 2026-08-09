# Block IDs Are Not Durable Across Restarts

## Summary

Block IDs come from `System.unique_integer([:positive, :monotonic])`, which is
a **per-VM counter that restarts near zero on every boot**. Block filenames and
the `blocks` primary key are both derived from it, so each restart replays low
IDs and overwrites the previous run's blocks — on disk and in the index.

This affects `raw`, `zstd`, and `openzl` blocks identically. It is not a
compression-format problem.

Found while investigating the 2026-08-09 `timelessmetrics.com` incident.

## Mechanism

`lib/timeless_traces/writer.ex:43` and `:83`, outside the format `case` and so
common to every format:

```elixir
block_id = System.unique_integer([:positive, :monotonic])
filename = "#{String.pad_leading(Integer.to_string(block_id), 12, "0")}.#{ext}"
file_path = Path.expand(Path.join([data_dir, "blocks", filename]))
```

`System.unique_integer/1` is documented as unique **within a running VM**. It
carries no cross-restart guarantee. After a restart the sequence begins again
at small positive integers.

Two things then collide:

1. **The file.** `File.write(file_path, data)` truncates and overwrites any
   block file already carrying that name from an earlier run.
2. **The index row.** `lib/timeless_traces/index.ex:718` and `:1050` use
   `INSERT OR REPLACE INTO blocks (block_id, ...)`, so the new row silently
   replaces the old block's row. The previous file, if it survived under a
   different name, is orphaned with nothing pointing at it.

## Production Evidence

On `tweb`, `/observability/spans`:

| Measure | Count |
|---|---|
| Files in `blocks/` | 1,934 |
| Zero-byte files | 1,748 |
| Non-zero `.ozl` files | 21 |
| Rows in the `blocks` index | **5** |

The index referenced blocks `660, 709, 710, 711, 712`. Minutes later the same
running system held files `742, 767, 768, 769, 770` — with byte sizes matching
the index rows exactly (92011, 82790, 81050, 84070). The same spans, rewritten
under fresh IDs after a restart, orphaning the previous generation.

The container had restarted **26,769 times** between 2026-07-02 and 2026-08-09
under a memory-pressure crash loop. That is the multiplier that turned this
defect into ~1,700 orphaned files.

## Zero-Byte Files

Most plausible cause is a block file created and then lost before its contents
were durably written, repeated across thousands of OOM-killed restarts. This is
consistent with the evidence but has **not** been directly confirmed — no write
path was instrumented. Treat it as a hypothesis.

What is confirmed: those files are not referenced by the index, so they are
orphans, not live data at risk.

## Impact

- **Silent data loss.** Blocks written before a restart can be destroyed by
  blocks written after it, with no error surfaced.
- **Unbounded orphan accumulation.** Files that lose their index row are never
  collected; `blocks/` grew to 1,934 files backing 5 live rows.
- **Misleading scale.** `blocks/` looks like ~40 MB of trace history. Live,
  index-referenced data is ~410 KB across 5 blocks (9,501 spans). Any recovery
  or migration effort sized against the directory is sized against orphans.

## Fix Direction

The durable-identity options, cheapest first:

1. **Derive the ID from something durable** — a persisted counter, the index's
   own `AUTOINCREMENT`, or a monotonic clock component plus a random suffix.
   Any of these removes the cross-restart collision.
2. **Refuse to overwrite.** Writing a block whose target path already exists
   should be an error, not a silent truncation. This turns a silent loss into a
   loud one even if the ID scheme regresses later.
3. **Reconcile orphans on startup** — report (do not auto-delete) block files
   with no index row, so the condition is visible.

The libSQL engine removes this class of bug structurally: a single database has
no per-block file identity to collide. That makes the migration the real fix,
and items 1–2 above worth doing only to protect stores that have not migrated
yet.

## Caveat

`created_at` on all five surviving rows decodes to `1970-01-01 00:00:01`, which
suggests a separate unit mismatch between what is written and how it is read
(`index.ex` writes `System.system_time(:second)`; the read path divides by
1e9). Not investigated further — noted so it is not mistaken for evidence about
block age.
