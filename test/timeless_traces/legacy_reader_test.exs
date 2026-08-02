defmodule TimelessTraces.LegacyReaderTest do
  use ExUnit.Case, async: false

  alias TimelessTraces.{DB, DB.Migrations, LegacyReader, Writer}

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

  def fixtures(count) do
    base = 1_700_000_000_000_000_000

    for index <- 0..(count - 1) do
      trace = div(index, 3) + 1
      first_span = div(index, 3) * 3 + 1
      span_id = index + 1

      %TimelessTraces.Span{
        trace_id: TimelessTraces.Span.encode_trace_id(trace),
        span_id: TimelessTraces.Span.encode_span_id(span_id),
        parent_span_id:
          if(rem(index, 3) == 0,
            do: nil,
            else: TimelessTraces.Span.encode_span_id(first_span)
          ),
        name: "span-#{index}",
        kind: Enum.at([:internal, :server, :client, :producer, :consumer], rem(index, 5)),
        start_time: base + index,
        end_time: base + index + 100 + index,
        duration_ns: 100 + index,
        status: Enum.at([:unset, :ok, :error], rem(index, 3)),
        status_message: if(rem(index, 3) == 2, do: "failure-#{index}", else: nil),
        attributes:
          %{
            "service.name" => "svc-#{rem(index, 2)}",
            "bool" => rem(index, 2) == 0,
            "nil" => nil,
            "nested" => %{"values" => [index, true]}
          }
          |> then(fn attributes ->
            if index == 5, do: Map.delete(attributes, "service.name"), else: attributes
          end),
        events: [
          %{
            "name" => "event-#{index}",
            "timeUnixNano" => Integer.to_string(base + index),
            "attributes" => %{"attempt" => index}
          }
        ],
        resource:
          if(index == 5,
            do: %{"host.name" => "node-a"},
            else: %{"service.name" => "resource-fallback", "host.name" => "node-a"}
          ),
        instrumentation_scope:
          if(rem(index, 2) == 0,
            do: %{"name" => "fixture", "version" => "1.0"},
            else: nil
          )
      }
    end
  end

  def create_sqlite_index(root, blocks) do
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(root, "traces_index.db"))
    Migrations.run(conn)

    Enum.each(blocks, fn block ->
      {:ok, _} =
        DB.execute(
          conn,
          "INSERT INTO blocks(block_id,file_path,byte_size,entry_count,ts_min,ts_max,format,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
          [
            block.block_id,
            block.file_path,
            block.byte_size,
            block.entry_count,
            block.ts_min,
            block.ts_max,
            Atom.to_string(block.format),
            System.system_time(:second)
          ]
        )
    end)

    Exqlite.Sqlite3.close(conn)
  end

  def block_row(block) do
    {
      block.block_id,
      block.file_path,
      block.byte_size,
      block.entry_count,
      block.ts_min,
      block.ts_max,
      block.format,
      System.system_time(:second)
    }
  end

  def block_map(block) do
    %{
      block_id: block.block_id,
      file_path: block.file_path,
      byte_size: block.byte_size,
      entry_count: block.entry_count,
      ts_min: block.ts_min,
      ts_max: block.ts_max,
      format: block.format
    }
  end

  def write_block(root, spans, format) do
    {:ok, block} = Writer.write_block(spans, root, format)
    block
  end

  def tree_snapshot(root) do
    root
    |> regular_files()
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  def regular_files(root) do
    root
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      path = Path.join(root, name)
      if File.dir?(path), do: regular_files(path), else: [path]
    end)
  end

  def temp_dir(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end
