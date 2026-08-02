defmodule TimelessTraces.ReleaseStartupTest do
  use ExUnit.Case, async: false

  alias TimelessTraces.{DB, LegacyReader, LegacyReaderTest, LibsqlCandidate, ReleaseStartup}

  test "fresh target is valid and startup remains idempotent" do
    root = LegacyReaderTest.temp_dir("traces_startup_fresh")
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, %{state: :fresh}} = ReleaseStartup.detect(root, opts())
    assert {:ok, %{state: :valid_libsql, ready: true}} = ReleaseStartup.prepare(root, opts())
    assert {:ok, %{state: :valid_libsql, ready: true}} = ReleaseStartup.prepare(root, opts())

    incompatible = LegacyReaderTest.temp_dir("traces_startup_incompatible_extension")
    on_exit(fn -> File.rm_rf!(incompatible) end)

    assert {:error, %{state: :incompatible_version, ready: false}} =
             ReleaseStartup.prepare(incompatible, extension_path: "/missing/timeless-ext.so")

    refute File.exists?(Path.join(incompatible, "traces.db"))
  end

  test "rich legacy spans resume, seal, rename, and retain an exact rollback source" do
    root = LegacyReaderTest.temp_dir("traces_startup_cutover")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(Path.join(root, "blocks"))
    block = LegacyReaderTest.write_block(root, LegacyReaderTest.fixtures(19), :raw)
    LegacyReaderTest.create_sqlite_index(root, [block])
    before = source_snapshot(root)

    assert {:ok, %{state: :legacy}} = ReleaseStartup.detect(root, opts())

    assert {:error, %{error: checkpoint_error}} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    assert checkpoint_error =~ "committed work is resumable"
    migration_before_detection = migration_fingerprint(root)
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert %{
             records_completed: 19,
             records_total: 19,
             candidate_physical_bytes: candidate_bytes,
             source_bytes: source_bytes
           } = ReleaseStartup.stats(root, opts())

    assert candidate_bytes > 0
    assert source_bytes > 0
    assert migration_fingerprint(root) == migration_before_detection

    assert {:error, _} =
             ReleaseStartup.prepare(root, Keyword.merge(opts(), failpoint: :before_rename))

    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())

    assert {:error, _} =
             ReleaseStartup.prepare(
               root,
               Keyword.merge(opts(), failpoint: :after_rename_before_fsync)
             )

    assert {:ok,
            %{
              state: :completed_cutover,
              source_retained: true,
              source_manifest_digest: digest,
              target_path: target
            }} =
             ReleaseStartup.detect(root, opts())

    assert source_snapshot(root) == before
    assert {:ok, legacy_reader} = LegacyReader.open(root)
    assert {:ok, %{records: 19}} = LegacyReader.inventory(legacy_reader)
    assert :ok = LegacyReader.close(legacy_reader)
    assert {:ok, conn, _} = LibsqlCandidate.open_connection(target, extension_path())

    try do
      assert {:ok, [[19]]} = DB.execute(conn, "SELECT COUNT(*) FROM traces", [])
    after
      Exqlite.Sqlite3.close(conn)
    end

    assert {:ok, %{source_retained: false}} = ReleaseStartup.cleanup_legacy(root, digest, opts())
    refute File.exists?(Path.join(root, "traces_index.db"))
    refute File.exists?(Path.join(root, "blocks"))

    assert {:ok, %{state: :completed_cutover, source_retained: false}} =
             ReleaseStartup.detect(root, opts())
  end

  test "drift, unlinked dual stores, future schema, and future journal are explicit" do
    drift = legacy_root("traces_startup_drift", 3)
    on_exit(fn -> File.rm_rf!(drift) end)

    assert {:error, _} =
             ReleaseStartup.prepare(
               drift,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    [block] = Path.wildcard(Path.join([drift, "blocks", "*.*"]))
    File.write!(block, "drift", [:append])
    assert {:ok, %{state: :corruption, error: drift_error}} = ReleaseStartup.detect(drift, opts())
    assert drift_error =~ "payload validation failed"

    dual = legacy_root("traces_startup_dual", 2)
    on_exit(fn -> File.rm_rf!(dual) end)
    target = Path.join(dual, "traces.db")
    create_target(target)
    assert {:ok, %{state: :ambiguous_dual_store}} = ReleaseStartup.detect(dual, opts())

    future = LegacyReaderTest.temp_dir("traces_startup_future")
    on_exit(fn -> File.rm_rf!(future) end)
    future_target = Path.join(future, "traces.db")
    create_target(future_target)
    {:ok, conn} = Exqlite.Sqlite3.open(future_target)

    assert {:ok, _} =
             DB.execute(
               conn,
               "INSERT INTO _timeless_schema_migrations VALUES ('traces',2,unixepoch(),'future','future',1)",
               []
             )

    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :incompatible_version}} = ReleaseStartup.detect(future, opts())

    journal = legacy_root("traces_startup_future_journal", 1)
    on_exit(fn -> File.rm_rf!(journal) end)

    assert {:error, _} =
             ReleaseStartup.prepare(
               journal,
               Keyword.merge(opts(), failpoint: {:after_checkpoint, 1})
             )

    candidate = TimelessTraces.ReleaseMigration.candidate_path(journal)
    {:ok, conn} = Exqlite.Sqlite3.open(candidate)
    assert {:ok, _} = DB.execute(conn, "UPDATE _timeless_migration SET version=2", [])
    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :incompatible_version}} = ReleaseStartup.detect(journal, opts())
  end

  test "corrupt files, wrong signal tables, and mixed old generations fail closed" do
    corrupt = LegacyReaderTest.temp_dir("traces_startup_corrupt")
    on_exit(fn -> File.rm_rf!(corrupt) end)
    File.write!(Path.join(corrupt, "traces.db"), "truncated")
    assert {:ok, %{state: :corruption}} = ReleaseStartup.detect(corrupt, opts())

    wrong = LegacyReaderTest.temp_dir("traces_startup_wrong")
    on_exit(fn -> File.rm_rf!(wrong) end)
    path = Path.join(wrong, "traces.db")
    {:ok, conn} = Exqlite.Sqlite3.open(path)
    load_extension(conn)
    assert {:ok, _} = DB.execute(conn, "CREATE VIRTUAL TABLE logs USING timeless_logs", [])
    Exqlite.Sqlite3.close(conn)
    assert {:ok, %{state: :corruption, error: error}} = ReleaseStartup.detect(wrong, opts())
    assert error =~ "wrong-signal"

    mixed = legacy_root("traces_startup_mixed", 1)
    on_exit(fn -> File.rm_rf!(mixed) end)

    File.write!(
      Path.join(mixed, "index.snapshot"),
      :erlang.term_to_binary(%{version: 1, timestamp: 0, blocks: []})
    )

    assert {:ok, %{state: :ambiguous_dual_store}} = ReleaseStartup.detect(mixed, opts())

    locked = legacy_root("traces_startup_legacy_owner", 1)
    on_exit(fn -> File.rm_rf!(locked) end)
    {:ok, conn} = Exqlite.Sqlite3.open(Path.join(locked, "traces_index.db"))
    assert {:ok, _} = DB.execute(conn, "BEGIN EXCLUSIVE", [])
    assert {:error, %{error: owner_error}} = ReleaseStartup.prepare(locked, opts())
    assert owner_error =~ "active legacy traces SQLite owner"
    assert {:ok, _} = DB.execute(conn, "ROLLBACK", [])
    Exqlite.Sqlite3.close(conn)

    missing = legacy_root("traces_startup_missing_block", 1)
    on_exit(fn -> File.rm_rf!(missing) end)
    [payload] = Path.wildcard(Path.join([missing, "blocks", "*.*"]))
    File.rm!(payload)

    assert {:ok, %{state: :corruption, error: missing_error}} =
             ReleaseStartup.detect(missing, opts())

    assert missing_error =~ "payload validation failed"
  end

  test "process kill after a sealed rich-span candidate resumes without touching the source" do
    root = legacy_root("traces_startup_kill", 5)
    on_exit(fn -> File.rm_rf!(root) end)
    before = source_snapshot(root)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          ReleaseStartup.prepare(
            root,
            Keyword.merge(opts(), pause_at: :after_seal, notify: parent)
          )

        send(parent, {:unexpected_startup_result, result})
      end)

    assert_receive {:startup_paused, ^pid, :after_seal}, 5_000
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000
    refute_receive {:unexpected_startup_result, _}
    assert {:ok, %{state: :resumable_migration}} = ReleaseStartup.detect(root, opts())
    assert {:ok, %{state: :completed_cutover}} = ReleaseStartup.prepare(root, opts())
    assert source_snapshot(root) == before
  end

  test "the oldest trace snapshot-only generation converts automatically" do
    root = LegacyReaderTest.temp_dir("traces_startup_snapshot_only")
    on_exit(fn -> File.rm_rf!(root) end)

    File.write!(
      Path.join(root, "index.snapshot"),
      :erlang.term_to_binary(
        %{
          version: 1,
          timestamp: 0,
          blocks: [],
          term_index: [],
          trace_index: [],
          compression_stats: []
        },
        [:compressed]
      )
    )

    assert {:ok, %{state: :legacy, generation: :snapshot_log, records_total: 0}} =
             ReleaseStartup.detect(root, opts())

    assert {:ok, %{state: :completed_cutover, source_retained: true}} =
             ReleaseStartup.prepare(root, opts())
  end

  defp legacy_root(prefix, count) do
    root = LegacyReaderTest.temp_dir(prefix)
    File.mkdir_p!(Path.join(root, "blocks"))
    block = LegacyReaderTest.write_block(root, LegacyReaderTest.fixtures(count), :raw)
    LegacyReaderTest.create_sqlite_index(root, [block])
    root
  end

  defp create_target(path) do
    {:ok, writer} = LibsqlCandidate.start_link(path: path, extension_path: extension_path())
    GenServer.stop(writer)
  end

  defp source_snapshot(root) do
    [Path.join(root, "traces_index.db"), Path.join(root, "blocks")]
    |> Enum.flat_map(&regular_files/1)
    |> Enum.sort()
    |> Enum.map(fn path ->
      stat = File.stat!(path, time: :posix)

      {Path.relative_to(path, root), stat.size, stat.mtime,
       :crypto.hash(:sha256, File.read!(path))}
    end)
  end

  defp migration_fingerprint(root) do
    path = TimelessTraces.ReleaseMigration.candidate_path(root)
    assert {:ok, conn, _} = LibsqlCandidate.open_readonly_connection(path, extension_path())

    try do
      for sql <- [
            "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
            "SELECT * FROM _timeless_migration",
            "SELECT * FROM _timeless_migration_events ORDER BY sequence",
            "SELECT COUNT(*) FROM traces"
          ] do
        assert {:ok, rows} = DB.execute(conn, sql, [])
        rows
      end
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp regular_files(path) do
    if File.dir?(path) do
      path |> File.ls!() |> Enum.flat_map(&regular_files(Path.join(path, &1)))
    else
      [path]
    end
  end

  defp load_extension(conn) do
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, true)
    assert {:ok, _} = DB.execute(conn, "SELECT load_extension(?1)", [extension_path()])
    :ok = Exqlite.Sqlite3.enable_load_extension(conn, false)
  end

  defp opts, do: [extension_path: extension_path()]

  defp extension_path do
    System.get_env("TIMELESS_EXT_PATH") ||
      Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)
  end
end
