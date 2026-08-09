# legacy_ozl_transcode

Recovers trace blocks written by `ex_openzl <= 0.4.6` so the current build can
read them.

## Why this exists

`ex_openzl 0.4.7` moved its vendored OpenZL from 0.1.x to v0.2.0. The 0.2.0
decoder rejects 0.1.x frames, so a store written before that upgrade cannot be
read by a build after it. The bytes are intact — the frames still introspect
cleanly and report `format_version: 24` — only the decoder is wrong. See
`docs/2026-08-09_legacy_migration_diagnostics.md` and, in the `ex_openzl` repo,
`docs/2026-08-09_openzl_0_2_0_frame_incompatibility.md`.

This blocks the legacy-to-libSQL migration outright: `validate_legacy_pages/4`
fails on the first block it cannot decode, and skipping blocks is deliberately
not an option — silently dropping them destroys the record of which data is
still in a deprecated format, which is exactly what you need in order to retire
that format.

Two OpenZL versions cannot be loaded into one BEAM: both NIFs are
`ex_openzl_nif.so` behind the same `ExOpenzl` module. So the conversion happens
out of process, here, and the output uses a format with no OpenZL dependency at
all.

## What it does

Decodes each `*.ozl` block with OpenZL 0.1.x and rewrites it as a `*.raw`
block — `:erlang.term_to_binary/1` over the same span maps. `:raw` is already
accepted by `TimelessTraces.Writer.decompress_block/2` and involves no
compression library, so it cannot go stale the same way.

Originals are never modified. Output goes to a separate directory.

Expect the files to get considerably larger: `:raw` is uncompressed, and a
65 KB block expands to roughly 1.1 MB.

## Requirements

A C++ toolchain and CMake, because the pinned `ex_openzl 0.4.6` is built from
source — the Hex package does not ship the vendored OpenZL tree. The dependency
is taken from git with `submodules: true`.

You do **not** need this toolchain on the production host. Copy the blocks off,
transcode locally, copy the results back.

## Use

```sh
cd tools/legacy_ozl_transcode
mix deps.get
mix legacy_ozl.transcode /path/to/blocks /path/to/output
```

It prints one line per block and then the `UPDATE` statements needed to point
`traces_index.db` at the new files. It does **not** run them: the index is live
data and the change should be applied deliberately, with the store stopped and
a backup taken.

If any block fails to decode the task exits non-zero and you should apply
nothing. A partial index update would leave rows pointing at files that do not
exist.

## Procedure

Every step below happens with the store **stopped**, and that is not just
about write safety. Block ids come from `System.unique_integer/1`, which
restarts near zero on every boot, so a restart renumbers the live blocks — the
same four sealed blocks were observed as ids 709–712 and later as 1368–1371.
The printed `UPDATE` statements key on `block_id`. If the store restarts
between reading the index and applying them, those ids no longer mean what
they meant, and the statements will miss their rows or hit the wrong ones.

Read the index and apply the updates inside one stopped window. See
`docs/2026-08-09_block_id_durability.md`.

1. Stop the traces store.
2. Back up `traces_index.db` and the `blocks/` directory.
3. Copy the `*.ozl` blocks that the index actually references. Note that the
   directory usually holds far more files than the index references — see
   `docs/2026-08-09_block_id_durability.md` — so work from the index:
   ```sql
   SELECT block_id, file_path FROM blocks WHERE format = 'openzl';
   ```
4. Run the task locally.
5. **Verify before applying.** Read the output back with the current build and
   confirm the spans are well formed:
   ```elixir
   {:ok, spans} = TimelessTraces.Writer.decompress_block(File.read!(path), :raw)
   ```
   Check that `trace_id` is non-empty, `start_time` is positive, `end_time` is
   at or after `start_time`, and `attributes`/`resource` are maps.
6. Copy the `*.raw` files into `blocks/`, leaving the originals in place.
7. Apply the printed `UPDATE` statements.
8. Restart. The libSQL migration can now read every block.

## Keeping in step with the writer

The columnar layout in `LegacyOzlTranscode` is vendored from
`TimelessTraces.Writer`, because this tool cannot depend on `timeless_traces`
without pulling in the newer `ex_openzl`. If the writer's column order or
rest-blob encoding changes, this must change with it.

Step 5 is the check that actually matters, and it is not optional: it is the
only thing that proves the vendored copy still matches the writer.

## Verified against production data

Three blocks taken from `timelessmetrics.com`, transcoded and then read back by
`timeless_traces 1.6.0` with `ex_openzl 0.4.16`:

| block | spans | malformed |
|---|---|---|
| `000000000025` | 184 | 0 |
| `000001949995` | 1853 | 0 |
| fixture copy | 184 | 0 |

2,221 spans recovered, all well formed, with intact trace ids, span names,
timestamps, and attributes.
