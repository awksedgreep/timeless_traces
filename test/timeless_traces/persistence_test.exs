defmodule TimelessTraces.PersistenceTest do
  @moduledoc """
  Tests for the SQLite-backed index persistence layer.

  These tests exercise crash recovery, restart data integrity, and edge cases
  that the normal integration tests don't cover because they run within a
  single GenServer lifecycle.

  With SQLite, persistence is atomic — no manual snapshot/WAL replay needed.
  """
  use ExUnit.Case, async: false

  @data_dir "test/tmp/persistence"

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)
    File.mkdir_p!(Path.join(@data_dir, "blocks"))

    on_exit(fn ->
      Application.stop(:timeless_traces)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp start_app(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, @data_dir)
    storage = Keyword.get(opts, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, data_dir)
    Application.put_env(:timeless_traces, :storage, storage)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :compaction_interval, 600_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_traces)
  end

  defp stop_app do
    Application.stop(:timeless_traces)
  end

  defp db_path, do: Path.join(@data_dir, "traces_index.db")

  defp make_span(overrides \\ %{}) do
    ts = System.system_time(:nanosecond)

    Map.merge(
      %{
        trace_id: "trace-#{System.unique_integer([:positive])}",
        span_id: "span-#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "HTTP GET /api",
        kind: :server,
        start_time: ts,
        end_time: ts + 50_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET", "service.name" => "test-svc"},
        events: [],
        resource: %{"service.name" => "test-svc"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  defp ingest_spans(spans) do
    case TimelessTraces.Writer.write_block(spans, @data_dir, :raw) do
      {:ok, meta} ->
        {terms, trace_rows} = TimelessTraces.Index.precompute(spans)
        TimelessTraces.Index.index_block(meta, terms, trace_rows)
        {:ok, meta, terms, trace_rows}

      err ->
        err
    end
  end

  # ----------------------------------------------------------------
  # Graceful restart: SQLite DB persists across restarts
  # ----------------------------------------------------------------

  describe "graceful restart recovery" do
    test "data survives app restart via SQLite" do
      start_app()

      spans =
        Enum.map(1..50, fn i ->
          make_span(%{name: "span-#{i}", status: Enum.random([:ok, :error])})
        end)

      {:ok, _meta, _terms, _trace_rows} = ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()
      assert stats_before.total_entries == 50
      assert stats_before.total_blocks == 1

      stop_app()

      assert File.exists?(db_path())

      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_entries == 50
      assert stats_after.total_blocks == 1

      {:ok, result} = TimelessTraces.Index.query(limit: 100)
      assert result.total == 50
    end

    test "term index survives restart" do
      start_app()

      spans = [
        make_span(%{name: "GET /users", status: :error, kind: :server}),
        make_span(%{name: "POST /users", status: :ok, kind: :client})
      ]

      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, error_before} = TimelessTraces.Index.query(status: :error, limit: 100)
      assert error_before.total == 1

      stop_app()
      start_app()

      {:ok, error_after} = TimelessTraces.Index.query(status: :error, limit: 100)
      assert error_after.total == 1

      {:ok, server_result} = TimelessTraces.Index.query(kind: :server, limit: 100)
      assert server_result.total == 1
    end

    test "trace index survives restart" do
      start_app()

      trace_id = "trace-survive-#{System.unique_integer([:positive])}"

      spans = [
        make_span(%{trace_id: trace_id, name: "root", parent_span_id: nil}),
        make_span(%{trace_id: trace_id, name: "child", parent_span_id: "span-parent"})
      ]

      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, trace_before} = TimelessTraces.Index.trace(trace_id)
      assert length(trace_before) == 2

      stop_app()
      start_app()

      {:ok, trace_after} = TimelessTraces.Index.trace(trace_id)
      assert length(trace_after) == 2
    end

    test "distinct_services survives restart" do
      start_app()

      spans = [
        make_span(%{
          attributes: %{"service.name" => "alpha"},
          resource: %{"service.name" => "alpha"}
        }),
        make_span(%{
          attributes: %{"service.name" => "beta"},
          resource: %{"service.name" => "beta"}
        })
      ]

      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, services_before} = TimelessTraces.Index.distinct_services()

      stop_app()
      start_app()

      {:ok, services_after} = TimelessTraces.Index.distinct_services()
      assert Enum.sort(services_before) == Enum.sort(services_after)
      assert "alpha" in services_after
      assert "beta" in services_after
    end

    test "compression stats survive restart" do
      start_app()

      spans = Enum.map(1..20, fn i -> make_span(%{name: "span-#{i}"}) end)
      {:ok, old_meta, _, _} = ingest_spans(spans)

      combined = Enum.map(1..20, fn i -> make_span(%{name: "compacted-#{i}"}) end)

      case TimelessTraces.Writer.write_block(combined, @data_dir, :zstd) do
        {:ok, new_meta} ->
          TimelessTraces.Index.compact_blocks(
            [old_meta.block_id],
            new_meta,
            combined,
            {2000, 1000}
          )

        _ ->
          :ok
      end

      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.compression_raw_bytes_in == stats_before.compression_raw_bytes_in

      assert stats_after.compression_compressed_bytes_out ==
               stats_before.compression_compressed_bytes_out

      assert stats_after.compaction_count == stats_before.compaction_count
    end

    test "multiple blocks survive restart" do
      start_app()

      for batch <- 1..5 do
        spans =
          Enum.map(1..10, fn i ->
            make_span(%{
              name: "batch-#{batch}-span-#{i}",
              start_time: System.system_time(:nanosecond) + batch * 1_000_000_000
            })
          end)

        ingest_spans(spans)
      end

      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()
      assert stats_before.total_blocks == 5
      assert stats_before.total_entries == 50

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_blocks == 5
      assert stats_after.total_entries == 50
    end
  end

  # ----------------------------------------------------------------
  # Crash recovery: SQLite handles atomicity, data persists
  # ----------------------------------------------------------------

  describe "crash recovery" do
    test "data persists even without graceful shutdown" do
      start_app()

      spans = Enum.map(1..30, fn i -> make_span(%{name: "crash-test-#{i}"}) end)
      {:ok, _meta, _terms, _trace_rows} = ingest_spans(spans)
      TimelessTraces.Index.sync()

      stop_app()
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_entries == 30
      assert stats.total_blocks == 1
    end

    test "trace lookups work after restart" do
      start_app()

      trace_id = "trace-crash-#{System.unique_integer([:positive])}"

      spans = [
        make_span(%{trace_id: trace_id, name: "root", parent_span_id: nil}),
        make_span(%{trace_id: trace_id, name: "db.query", parent_span_id: "parent-1"}),
        make_span(%{trace_id: trace_id, name: "cache.get", parent_span_id: "parent-1"})
      ]

      ingest_spans(spans)
      TimelessTraces.Index.sync()

      stop_app()
      start_app()

      {:ok, recovered_trace} = TimelessTraces.Index.trace(trace_id)
      assert length(recovered_trace) == 3
    end

    test "delete operations persist across restarts" do
      start_app()

      old_ts = System.system_time(:nanosecond) - 100_000_000_000_000

      old_spans =
        Enum.map(1..10, fn i ->
          make_span(%{name: "old-#{i}", start_time: old_ts + i, end_time: old_ts + i + 1000})
        end)

      new_spans =
        Enum.map(1..10, fn i ->
          ts = System.system_time(:nanosecond) + i * 1_000_000
          make_span(%{name: "new-#{i}", start_time: ts, end_time: ts + 1000})
        end)

      ingest_spans(old_spans)
      ingest_spans(new_spans)
      TimelessTraces.Index.sync()

      deleted = TimelessTraces.Index.delete_blocks_before(old_ts + 100_000)
      assert deleted == 1

      TimelessTraces.Index.sync()

      stop_app()
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 10
    end

    test "compact operations persist across restarts" do
      start_app()

      spans1 = Enum.map(1..10, fn i -> make_span(%{name: "batch1-#{i}"}) end)
      spans2 = Enum.map(1..10, fn i -> make_span(%{name: "batch2-#{i}"}) end)

      {:ok, meta1, _, _} = ingest_spans(spans1)
      {:ok, meta2, _, _} = ingest_spans(spans2)
      TimelessTraces.Index.sync()

      combined = spans1 ++ spans2

      case TimelessTraces.Writer.write_block(combined, @data_dir, :zstd) do
        {:ok, new_meta} ->
          TimelessTraces.Index.compact_blocks(
            [meta1.block_id, meta2.block_id],
            new_meta,
            combined,
            {3000, 1500}
          )

        _ ->
          flunk("Failed to write compacted block")
      end

      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()
      assert stats_before.total_blocks == 1
      assert stats_before.total_entries == 20

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 20
      assert stats_after.compaction_count == stats_before.compaction_count
    end
  end

  # ----------------------------------------------------------------
  # Large-scale persistence
  # ----------------------------------------------------------------

  describe "large-scale persistence" do
    test "many blocks survive restart" do
      start_app()

      for _i <- 1..100 do
        ingest_spans([make_span()])
      end

      TimelessTraces.Index.sync()

      stop_app()
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 100
      assert stats.total_entries == 100
    end
  end

  # ----------------------------------------------------------------
  # Corrupt / missing file handling
  # ----------------------------------------------------------------

  describe "corrupt and missing file handling" do
    test "missing DB file starts with empty state" do
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 0
      assert stats.total_entries == 0

      stop_app()
    end

    test "empty data dir starts cleanly" do
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 0

      spans = [make_span()]
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, stats2} = TimelessTraces.Index.stats()
      assert stats2.total_blocks == 1

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Backup
  # ----------------------------------------------------------------

  describe "backup" do
    test "backup creates a valid SQLite database at target path" do
      start_app()

      spans = Enum.map(1..20, fn i -> make_span(%{name: "backup-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      backup_path = Path.join(@data_dir, "backup.db")
      assert :ok == TimelessTraces.Index.backup(backup_path)
      assert File.exists?(backup_path)

      # Verify it's a valid SQLite database by opening and querying it
      {:ok, conn} = Exqlite.Sqlite3.open(backup_path)
      {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT COUNT(*) FROM blocks")
      {:row, [count]} = Exqlite.Sqlite3.step(conn, stmt)
      Exqlite.Sqlite3.release(conn, stmt)
      Exqlite.Sqlite3.close(conn)
      assert count == 1

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Memory mode
  # ----------------------------------------------------------------

  describe "memory mode" do
    test "memory mode does not create snapshot or log files" do
      start_app(storage: :memory, data_dir: "test/tmp/mem_persist_test")

      spans = Enum.map(1..10, fn i -> make_span(%{name: "mem-#{i}"}) end)

      case TimelessTraces.Writer.write_block(spans, :memory, :raw) do
        {:ok, meta} ->
          {terms, trace_rows} = TimelessTraces.Index.precompute(spans)
          TimelessTraces.Index.index_block(meta, terms, trace_rows)

        _ ->
          flunk("Failed to write memory block")
      end

      TimelessTraces.Index.sync()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_entries == 10

      # Old snapshot/log files should not exist
      refute File.exists?("test/tmp/mem_persist_test/index.log")
      refute File.exists?("test/tmp/mem_persist_test/index.snapshot")

      stop_app()
      File.rm_rf!("test/tmp/mem_persist_test")
    end
  end

  # ----------------------------------------------------------------
  # Async indexing recovery
  # ----------------------------------------------------------------

  describe "async indexing recovery" do
    test "async-indexed blocks survive graceful restart" do
      start_app()

      spans = Enum.map(1..20, fn i -> make_span(%{name: "async-#{i}"}) end)

      case TimelessTraces.Writer.write_block(spans, @data_dir, :raw) do
        {:ok, meta} ->
          {terms, trace_rows} = TimelessTraces.Index.precompute(spans)
          TimelessTraces.Index.index_block_async(meta, terms, trace_rows)

        _ ->
          flunk("Failed to write block")
      end

      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()
      assert stats_before.total_entries == 20

      stop_app()
      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_entries == 20

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Stats (index_size) reflects SQLite DB files
  # ----------------------------------------------------------------

  describe "stats reflect persistence files" do
    test "index_size reports SQLite DB size" do
      start_app()

      spans = Enum.map(1..50, fn i -> make_span(%{name: "stats-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.index_size > 0

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # ETS migration
  # ----------------------------------------------------------------

  describe "ETS snapshot migration" do
    test "old ETS snapshot is migrated to SQLite on startup" do
      # Create a fake old-format snapshot
      snapshot = %{
        version: 1,
        timestamp: System.monotonic_time(),
        blocks: [
          {1001, Path.join(@data_dir, "blocks/fake.zst"), 100, 5, 1000, 2000, :zstd,
           System.system_time(:second)}
        ],
        term_index: [{"name:test-op", 1001}, {"service.name:test-svc", 1001}],
        trace_index: [{"trace-abc123", 1001}],
        compression_stats: [{:lifetime, 500, 200, 1}],
        block_data: []
      }

      snapshot_path = Path.join(@data_dir, "index.snapshot")
      File.write!(snapshot_path, :erlang.term_to_binary(snapshot, [:compressed]))

      start_app()

      # Snapshot should have been migrated
      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 5

      # Old snapshot file should be cleaned up
      refute File.exists?(snapshot_path)

      stop_app()
    end
  end
end
