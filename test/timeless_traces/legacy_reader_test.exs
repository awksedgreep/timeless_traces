defmodule TimelessTraces.LegacyReaderTest do
  use ExUnit.Case, async: false

  alias TimelessTraces.{LegacyReader, Writer}
  import TimelessTraces.LegacyReaderFixture

  test "SQLite generation decodes raw, zstd, and OpenZL one block at a time without mutation" do
    root = temp_dir("traces_sqlite_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    spans = fixtures(6)

    blocks = [
      write_block(root, Enum.slice(spans, 0, 2), :raw),
      write_block(root, Enum.slice(spans, 2, 2), :zstd),
      write_block(root, Enum.slice(spans, 4, 2), :openzl)
    ]

    create_sqlite_index(root, blocks)
    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :sqlite)
    assert {:ok, %{blocks: 3, records: 6}} = LegacyReader.inventory(reader)
    assert {:ok, first, cursor, true} = LegacyReader.page(reader, nil, 2)
    assert {:ok, second, cursor, true} = LegacyReader.page(reader, cursor, 2)
    assert {:ok, third, _cursor, false} = LegacyReader.page(reader, cursor, 2)
    assert first ++ second ++ third == spans
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before

    assert {:ok, reader} = LegacyReader.open(root, generation: :sqlite, max_stored_bytes: 1)
    assert {:error, {:oversized, _path, _size, 1}} = LegacyReader.page(reader, nil, 2)
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end

  test "snapshot disk-log replay applies delete and compact terms read-only" do
    root = temp_dir("traces_snapshot_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    [first, second, third | _] = fixtures(3)
    old = write_block(root, [first], :raw)
    replacement = write_block(root, [second, third], :zstd)

    snapshot = %{
      version: 1,
      timestamp: 100,
      blocks: [block_row(old)],
      term_index: [],
      trace_index: [],
      compression_stats: [],
      block_data: []
    }

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(snapshot, [:compressed])
    )

    name = :timeless_traces_legacy_reader_fixture

    {:ok, ^name} =
      :disk_log.open(
        name: name,
        file: String.to_charlist(Path.join(root, "index.log")),
        type: :halt,
        format: :internal
      )

    :ok =
      :disk_log.log(
        name,
        {:compact_blocks, 101, [old.block_id], block_map(replacement), [], [], {1, 1}}
      )

    :ok = :disk_log.sync(name)
    :ok = :disk_log.close(name)
    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :snapshot_log)
    assert {:ok, %{blocks: 1, records: 2}} = LegacyReader.inventory(reader)
    assert {:ok, spans, _cursor, false} = LegacyReader.page(reader)
    assert spans == [second, third]
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end

  test "snapshot generation reads legacy inline rich spans immutably" do
    root = temp_dir("traces_snapshot_inline_reader")
    on_exit(fn -> File.rm_rf!(root) end)
    [span | _] = fixtures(1)
    {:ok, block} = Writer.write_block([span], :memory, :raw)

    snapshot = %{
      version: 1,
      timestamp: 100,
      blocks: [block_row(block)],
      term_index: [],
      trace_index: [],
      compression_stats: [],
      block_data: [{block.block_id, block.data}]
    }

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(snapshot, [:compressed])
    )

    before = tree_snapshot(root)

    assert {:ok, reader} = LegacyReader.open(root, generation: :snapshot_log)
    assert {:ok, [^span], _cursor, false} = LegacyReader.page(reader)
    assert :ok = LegacyReader.close(reader)
    assert tree_snapshot(root) == before
  end
end
