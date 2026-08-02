defmodule TimelessTraces.LegacyReaderFixture do
  @moduledoc false

  alias TimelessTraces.{DB, DB.Migrations, Writer}

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
