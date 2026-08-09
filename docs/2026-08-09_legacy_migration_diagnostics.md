# Legacy Migration Reports Version Incompatibility as Data Corruption

## Summary

When the legacy-to-libSQL migration meets a block it cannot decode, it reports
`state: :corruption` regardless of why. A block written by an older, no longer
readable compression library is indistinguishable in the output from a block
whose bytes are damaged.

In the 2026-08-09 `timelessmetrics.com` incident this sent the investigation
after disk corruption that did not exist. The real cause was an `ex_openzl`
format break (see
`ex_openzl/docs/2026-08-09_openzl_0_2_0_frame_incompatibility.md`).

## The Failure Path

```
Writer.decompress_block(data, :openzl)          writer.ex:142
  rescue e -> Logger.warning(...)               writer.ex:149   <-- detail logged
             {:error, :corrupt_block}           writer.ex:150   <-- detail DISCARDED
    |
LegacyReader.decode_block/2                     legacy_reader.ex:395
    |
ReleaseStartup.validate_legacy_pages/4          release_startup.ex:513
  {:error, "legacy traces payload validation failed: :corrupt_block"}
    |
ReleaseStartup.classify_legacy_error/1          release_startup.ex:530
  String.contains?(reason, "unsupported") -> :incompatible_version
  otherwise                               -> :corruption
```

There are two independent defects here.

**1. The reason is destroyed before it can be classified.**
`decompress_block/2` rescues the underlying error, logs it, and returns the
bare atom `:corrupt_block`. Everything OpenZL said about *why* — including
whether the frame was well-formed — is gone by the time classification runs.

This is the load-bearing defect. Editing `classify_legacy_error/1` alone
changes nothing, because the string it inspects can never contain the decoder's
explanation.

**2. Classification keys on a substring that never appears.**
`classify_legacy_error/1` returns `:incompatible_version` only when the reason
contains `"unsupported"`. OpenZL's own wording for a version mismatch is
`"Corruption detected"`, so even with the detail preserved, the current
heuristic would still answer `:corruption`.

## Why This Matters Beyond Cosmetics

`:corruption` and `:incompatible_version` imply opposite operator responses.
Corruption means the bytes are lost and the operator should restore from
backup. Incompatible version means the bytes are intact and need a different
reader. Reporting the first when the second is true sends operators toward
destructive recovery for data that was never damaged.

Both states are dead ends in `run_detected/3` (`release_startup.ex:171`), so
today the distinction changes no control flow. It is purely diagnostic — which
is precisely why it must be accurate.

## The Discriminator

The obvious discriminators were tried first and **both failed measurement**:

- **`format_version`** is `24` for legacy 0.1.x frames *and* for frames written
  by the current build. The field does not move across this break.
- **"does `frame_info/1` still parse it?"** is not sufficient either. A frame
  with 64 payload bytes overwritten, and a frame truncated to a third of its
  length, both still introspect cleanly. Classifying on introspection alone
  reported those as a version problem — which is worse than the bug being
  fixed, because it tells an operator that genuinely lost data is fine.

What does separate the cases is the **error class**. OpenZL checksums the
compressed payload, so damage announces itself:

| Input | decoder error class | verdict |
|---|---|---|
| Legacy 0.1.x block | `streams to regenerate` / `Graph inconsistency` | unreadable format |
| Payload overwritten | `Compressed checksum mismatch` | corruption |
| Truncated | `Source size too small` | corruption |
| Not an OpenZL frame | `frame_info` itself fails | corruption |

A topology error means the container parsed but the compression graph inside it
could not be interpreted — that is what an older OpenZL's output looks like to a
newer decoder.

The implementation is an **allowlist**: only positively recognised topology
signatures become `:incompatible_format`, and every other failure keeps the
historical `:corrupt_block` answer. A denylist would silently reclassify future
unknown decoder errors as "intact", which is the direction that gets data
thrown away.

## Fix

1. Preserve the decode failure reason instead of collapsing it to
   `:corrupt_block`. `decompress_block/2` returns `{:error, :incompatible_format}`
   when `frame_info/1` succeeds but decoding fails, and keeps
   `{:error, :corrupt_block}` otherwise.
2. Teach `classify_legacy_error/1` to map that to `:incompatible_version`
   explicitly, rather than sniffing for `"unsupported"`.

The public `{:error, :corrupt_block}` contract is preserved for genuinely
damaged input, so existing callers (`index.ex:997`,
`mix/tasks/compression_benchmark.ex:53`, `writer_test.exs:126,132`) are
unaffected.

### Deliberately Not Doing

Quarantining undecodable blocks and continuing the migration was considered and
rejected. Silently skipping blocks destroys the record of which data still sits
in a deprecated format — exactly the signal needed to retire a compression
format safely. The migration should stay all-or-nothing and the blocks should
be made readable instead.

## Scope Note

On the production store this affects **5 blocks totalling ~410 KB**
(9,501 spans), not the 40 MB the `blocks/` directory suggests. See
`2026-08-09_block_id_durability.md` for why those two numbers disagree.
