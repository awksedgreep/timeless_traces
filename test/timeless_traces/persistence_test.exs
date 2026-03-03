defmodule TimelessTraces.PersistenceTest do
  @moduledoc """
  Tests for the disk_log + ETS snapshot persistence layer.

  These tests exercise crash recovery, log replay, snapshot correctness,
  and edge cases that the normal integration tests don't cover because
  they run within a single GenServer lifecycle.

  Crash recovery is simulated by: graceful stop (writes snapshot + keeps log),
  then deleting the snapshot, then restarting (forces log-only replay).
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

  defp snapshot_path, do: Path.join(@data_dir, "index.snapshot")
  defp log_path, do: Path.join(@data_dir, "index.log")

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
  # Graceful restart: snapshot written on terminate, loaded on init
  # ----------------------------------------------------------------

  describe "graceful restart recovery" do
    test "data survives app restart via snapshot" do
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

      assert File.exists?(snapshot_path())

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
  # Log-only recovery: simulate crash by deleting snapshot after stop
  # ----------------------------------------------------------------

  describe "crash recovery via log replay" do
    test "data recoverable from log when snapshot is lost" do
      start_app()

      spans = Enum.map(1..30, fn i -> make_span(%{name: "crash-test-#{i}"}) end)
      {:ok, _meta, _terms, _trace_rows} = ingest_spans(spans)
      TimelessTraces.Index.sync()

      # Graceful stop writes snapshot and keeps log
      stop_app()

      # Delete snapshot to force log-only replay
      File.rm!(snapshot_path())
      refute File.exists?(snapshot_path())
      assert File.exists?(log_path())

      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_entries == 30
      assert stats.total_blocks == 1
    end

    test "trace lookups work after log-only recovery" do
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
      File.rm!(snapshot_path())

      start_app()

      {:ok, recovered_trace} = TimelessTraces.Index.trace(trace_id)
      assert length(recovered_trace) == 3
    end

    test "delete operations in log are replayed correctly" do
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

      # Stop gracefully, then remove snapshot to force log replay
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 10
    end

    test "compact operations in log are replayed correctly" do
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

      # Stop gracefully, then remove snapshot to force log replay
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 20
      assert stats_after.compaction_count == stats_before.compaction_count
    end
  end

  # ----------------------------------------------------------------
  # Snapshot + log interaction
  # ----------------------------------------------------------------

  describe "snapshot + log interaction" do
    test "entries written after snapshot are recovered from log" do
      start_app()

      spans1 = Enum.map(1..10, fn i -> make_span(%{name: "pre-snapshot-#{i}"}) end)
      ingest_spans(spans1)
      TimelessTraces.Index.sync()

      # Graceful stop writes snapshot (batch 1 only)
      stop_app()

      # Save old snapshot
      old_snapshot = File.read!(snapshot_path())

      # Restart and write second batch
      start_app()
      spans2 = Enum.map(1..10, fn i -> make_span(%{name: "post-snapshot-#{i}"}) end)
      ingest_spans(spans2)
      TimelessTraces.Index.sync()

      {:ok, stats_running} = TimelessTraces.Index.stats()
      assert stats_running.total_entries == 20

      # Graceful stop writes new snapshot, log has all entries
      stop_app()

      # Restore old snapshot — log still has batch 2 entries
      File.write!(snapshot_path(), old_snapshot)

      # Restart: loads old snapshot (batch 1) + replays log (batch 2)
      start_app()

      {:ok, stats_recovered} = TimelessTraces.Index.stats()
      assert stats_recovered.total_entries == 20
      assert stats_recovered.total_blocks == 2
    end

    test "log entries already in snapshot are not double-applied" do
      start_app()

      spans = Enum.map(1..10, fn i -> make_span(%{name: "idempotent-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      stop_app()
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_entries == 10
      assert stats.total_blocks == 1
    end

    test "mixed operations in log replay: index + delete + index" do
      start_app()

      old_ts = System.system_time(:nanosecond) - 100_000_000_000_000

      old_spans =
        Enum.map(1..5, fn i ->
          make_span(%{
            name: "old-#{i}",
            start_time: old_ts + i,
            end_time: old_ts + i + 1000
          })
        end)

      ingest_spans(old_spans)
      TimelessTraces.Index.sync()

      # Graceful stop writes snapshot with old entries
      stop_app()

      # Save old snapshot
      old_snapshot = File.read!(snapshot_path())

      start_app()

      # Delete old entries (logged to disk_log)
      TimelessTraces.Index.delete_blocks_before(old_ts + 100_000)
      TimelessTraces.Index.sync()

      # Index new entries (also logged)
      new_spans =
        Enum.map(1..5, fn i ->
          ts = System.system_time(:nanosecond) + i * 1_000_000
          make_span(%{name: "new-#{i}", start_time: ts, end_time: ts + 1000})
        end)

      ingest_spans(new_spans)
      TimelessTraces.Index.sync()

      {:ok, stats_before} = TimelessTraces.Index.stats()
      assert stats_before.total_blocks == 1
      assert stats_before.total_entries == 5

      # Stop gracefully, restore old snapshot to force log replay of delete + new index
      stop_app()
      File.write!(snapshot_path(), old_snapshot)

      start_app()

      {:ok, stats_after} = TimelessTraces.Index.stats()
      assert stats_after.total_blocks == 1
      assert stats_after.total_entries == 5
    end
  end

  # ----------------------------------------------------------------
  # Snapshot threshold (auto-snapshot after N log entries)
  # ----------------------------------------------------------------

  describe "snapshot threshold" do
    test "snapshot is triggered after threshold log entries" do
      start_app()

      for _i <- 1..1100 do
        ingest_spans([make_span()])
      end

      TimelessTraces.Index.sync()

      # Snapshot should have been written at entry 1000
      assert File.exists?(snapshot_path())

      # Graceful restart should recover all data (snapshot + log replay)
      stop_app()
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 1100
      assert stats.total_entries == 1100
    end

    test "log is truncated after auto-snapshot" do
      start_app()

      # Write 1100 blocks: auto-snapshot fires at 1000, truncates log
      for _i <- 1..1100 do
        ingest_spans([make_span()])
      end

      TimelessTraces.Index.sync()

      # Stop, delete snapshot — only post-truncation log entries survive
      stop_app()
      File.rm!(snapshot_path())

      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      # Only the 100 entries written after the auto-snapshot truncation are in the log
      assert stats.total_blocks == 100
      assert stats.total_entries == 100
    end
  end

  # ----------------------------------------------------------------
  # Corrupt / missing file handling
  # ----------------------------------------------------------------

  describe "corrupt and missing file handling" do
    test "missing snapshot file starts with empty state" do
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 0
      assert stats.total_entries == 0

      stop_app()
    end

    test "corrupt snapshot file recovers from log" do
      start_app()

      spans = Enum.map(1..10, fn i -> make_span(%{name: "corrupt-test-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      stop_app()

      # Corrupt the snapshot
      File.write!(snapshot_path(), "this is not valid erlang term binary")

      # Restart — should handle corrupt snapshot gracefully and replay from log
      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 10

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

    test "partial snapshot (.tmp file) does not corrupt state" do
      start_app()

      spans = Enum.map(1..10, fn i -> make_span(%{name: "tmp-test-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      stop_app()

      tmp_path = snapshot_path() <> ".tmp"
      File.write!(tmp_path, "partial garbage data")

      start_app()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.total_entries == 10

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Backup
  # ----------------------------------------------------------------

  describe "backup" do
    test "backup creates a valid snapshot at target path" do
      start_app()

      spans = Enum.map(1..20, fn i -> make_span(%{name: "backup-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      backup_path = Path.join(@data_dir, "backup.snapshot")
      assert :ok == TimelessTraces.Index.backup(backup_path)
      assert File.exists?(backup_path)

      binary = File.read!(backup_path)
      snapshot = :erlang.binary_to_term(binary)
      assert snapshot.version == 1
      assert length(snapshot.blocks) == 1
      assert length(snapshot.term_index) > 0
      assert length(snapshot.trace_index) > 0

      stop_app()
    end
  end

  # ----------------------------------------------------------------
  # Memory mode
  # ----------------------------------------------------------------

  describe "memory mode" do
    test "memory mode skips log and snapshot" do
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

      refute File.exists?("test/tmp/mem_persist_test/index.log")
      refute File.exists?("test/tmp/mem_persist_test/index.snapshot")

      stop_app()
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
  # Stats reflect new persistence files
  # ----------------------------------------------------------------

  describe "stats reflect persistence files" do
    test "index_size reports snapshot + log size" do
      start_app()

      spans = Enum.map(1..50, fn i -> make_span(%{name: "stats-#{i}"}) end)
      ingest_spans(spans)
      TimelessTraces.Index.sync()

      {:ok, stats} = TimelessTraces.Index.stats()
      assert stats.index_size > 0

      stop_app()
    end
  end
end
