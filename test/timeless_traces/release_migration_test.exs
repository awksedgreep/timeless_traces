defmodule TimelessTraces.ReleaseMigrationTest do
  use ExUnit.Case, async: false

  alias TimelessTraces.{DB, LegacyReaderTest, LibsqlCandidate}

  test "rich spans resume across every 8,192 checkpoint crash boundary with exact relationships" do
    root = LegacyReaderTest.temp_dir("traces_release_migration")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    spans = LegacyReaderTest.fixtures(8_193)
    block = LegacyReaderTest.write_block(root, spans, :raw)
    LegacyReaderTest.create_sqlite_index(root, [block])
    before = source_snapshot(root)

    assert {:error, disk_error} = TimelessTraces.ReleaseMigration.stage(root, available_bytes: 0)
    assert disk_error =~ "insufficient disk"
    refute File.exists?(TimelessTraces.ReleaseMigration.candidate_path(root))

    for point <- [
          :before_batch,
          :disk_full,
          :after_batch_before_journal,
          :after_journal_before_commit
        ] do
      assert {:error, error} =
               TimelessTraces.ReleaseMigration.stage(root,
                 failpoint: {point, 1},
                 extension_path: extension_path()
               )

      assert error =~ "injected migration failure"
      assert journal_count(root) == 0
      assert source_snapshot(root) == before
    end

    assert {:error, error} =
             TimelessTraces.ReleaseMigration.stage(root,
               failpoint: {:after_checkpoint, 1},
               extension_path: extension_path()
             )

    assert error =~ "committed work is resumable"
    assert journal_count(root) == 8_192
    assert source_snapshot(root) == before

    assert {:ok, report} =
             TimelessTraces.ReleaseMigration.stage(root, extension_path: extension_path())

    assert report.phase == :verified
    assert report.spans == 8_193
    assert report.checkpoints == 2
    assert report.retries == 5
    assert report.wal_bytes == 0
    assert report.candidate_bytes > 0
    assert report.process_hwm_bytes > 0
    assert source_snapshot(root) == before

    assert {:ok, conn, _} =
             LibsqlCandidate.open_connection(
               TimelessTraces.ReleaseMigration.candidate_path(root),
               extension_path()
             )

    try do
      trace_id = TimelessTraces.Span.encode_trace_id(1) |> Base.decode16!(case: :mixed)

      assert {:ok, rows} =
               DB.execute(
                 conn,
                 "SELECT span_id,parent_span_id,name,kind,status,status_description,attributes,events,resource,instrumentation_scope FROM traces WHERE trace_id=?1 ORDER BY start_ts ASC,span_id ASC",
                 [{:blob, trace_id}]
               )

      assert length(rows) == 3
      assert Enum.map(rows, &Enum.at(&1, 2)) == ["span-0", "span-1", "span-2"]
      assert Enum.at(Enum.at(rows, 0), 1) == nil

      assert Enum.at(Enum.at(rows, 1), 1) ==
               TimelessTraces.Span.encode_span_id(1) |> Base.decode16!(case: :mixed)

      assert Enum.at(Enum.at(rows, 2), 5) == "failure-2"

      attributes = rows |> hd() |> Enum.at(6) |> :json.decode()
      assert attributes["bool"] == true
      assert attributes["nil"] == :null
      assert attributes["nested"] == %{"values" => [0, true]}

      events = rows |> hd() |> Enum.at(7) |> :json.decode()
      assert hd(events)["name"] == "event-0"
      assert rows |> Enum.at(1) |> Enum.at(9) == "{}"

      assert {:ok, [[""]]} =
               DB.execute(conn, "SELECT service FROM traces WHERE name='span-5'", [])

      assert {:ok, services} =
               DB.execute(
                 conn,
                 "SELECT value FROM timeless_trace_services('traces') ORDER BY value",
                 []
               )

      refute [""] in services
    after
      Exqlite.Sqlite3.close(conn)
    end

    assert {:ok, retry} =
             TimelessTraces.ReleaseMigration.stage(root, extension_path: extension_path())

    assert retry.spans == report.spans
    assert retry.identity_digest == report.identity_digest
    assert retry.relationship_digest == report.relationship_digest
    assert retry.checkpoints == report.checkpoints
    assert retry.retries == report.retries + 1
    assert source_snapshot(root) == before
  end

  test "snapshot plus disk-log traces generation reaches the same cold oracle" do
    root = LegacyReaderTest.temp_dir("traces_snapshot_migration")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    [first, second, third | _] = LegacyReaderTest.fixtures(3)
    old = LegacyReaderTest.write_block(root, [first], :raw)
    replacement = LegacyReaderTest.write_block(root, [second, third], :zstd)

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(
        %{
          version: 1,
          timestamp: 100,
          blocks: [LegacyReaderTest.block_row(old)],
          term_index: [],
          trace_index: [],
          compression_stats: [],
          block_data: []
        },
        [:compressed]
      )
    )

    name = :timeless_traces_release_snapshot_fixture

    {:ok, ^name} =
      :disk_log.open(
        name: name,
        file: String.to_charlist(Path.join(root, "index.log")),
        type: :halt,
        format: :internal
      )

    :ok = :disk_log.log(name, {:delete_blocks, 101, [old.block_id]})

    :ok =
      :disk_log.log(
        name,
        {:index_block, 102, LegacyReaderTest.block_map(replacement), [], []}
      )

    :ok = :disk_log.sync(name)
    :ok = :disk_log.close(name)
    before = source_snapshot(root)

    assert {:ok, report} =
             TimelessTraces.ReleaseMigration.stage(root,
               generation: :snapshot_log,
               extension_path: extension_path()
             )

    assert report.spans == 2
    assert report.phase == :verified
    assert source_snapshot(root) == before
  end

  test "inconsistent rich span end-time fails closed instead of dropping fidelity" do
    root = LegacyReaderTest.temp_dir("traces_invalid_rich_span")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    [span | _] = LegacyReaderTest.fixtures(1)
    span = %{span | end_time: span.end_time + 1}
    block = LegacyReaderTest.write_block(root, [span], :raw)
    LegacyReaderTest.create_sqlite_index(root, [block])
    before = source_snapshot(root)

    assert {:error, error} =
             TimelessTraces.ReleaseMigration.stage(root, extension_path: extension_path())

    assert error =~ "end_time does not equal start_time + duration_ns"
    assert journal_count(root) == 0
    assert source_snapshot(root) == before
  end

  test "fresh migration reports scan, public write, maintenance, storage, and HWM costs" do
    root = LegacyReaderTest.temp_dir("traces_release_migration_benchmark")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    block = LegacyReaderTest.write_block(root, LegacyReaderTest.fixtures(8_193), :raw)
    LegacyReaderTest.create_sqlite_index(root, [block])

    assert {:ok, report} =
             TimelessTraces.ReleaseMigration.stage(root, extension_path: extension_path())

    assert report.spans == 8_193
    assert report.source_scan_ns > 0
    assert report.public_write_ns > 0
    assert report.optimize_ns > 0
    assert report.checkpoint_ns > 0
    assert report.physical_bytes >= report.candidate_bytes
    assert report.process_hwm_bytes > 0

    if System.get_env("TIMELESS_MIGRATION_BENCH") == "1",
      do: IO.inspect(report, label: "traces migration benchmark")
  end

  defp journal_count(root) do
    {:ok, conn} =
      Exqlite.Sqlite3.open(TimelessTraces.ReleaseMigration.candidate_path(root), mode: :readonly)

    try do
      {:ok, [[count]]} =
        DB.execute(
          conn,
          "SELECT records_completed FROM _timeless_migration WHERE singleton=1",
          []
        )

      count
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp source_snapshot(root) do
    root
    |> LegacyReaderTest.regular_files()
    |> Enum.reject(&String.contains?(&1, "/.timeless-migration/"))
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  defp extension_path do
    System.get_env("TIMELESS_EXT_PATH") ||
      Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)
  end
end
