defmodule TimelessTraces.CompactorTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/compactor_#{System.unique_integer([:positive])}"

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, @data_dir)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.put_env(:timeless_traces, :compaction_threshold, 10)
    Application.put_env(:timeless_traces, :compaction_max_raw_age, 60)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      File.rm_rf!(@data_dir)
      Application.put_env(:timeless_traces, :storage, :disk)
      Application.put_env(:timeless_traces, :compaction_threshold, 500)
      Application.put_env(:timeless_traces, :compaction_max_raw_age, 60)
    end)

    :ok
  end

  defp make_span(overrides \\ %{}) do
    Map.merge(
      %{
        trace_id: "trace-#{System.unique_integer([:positive])}",
        span_id: "span-#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "HTTP GET /api",
        kind: :server,
        start_time: System.system_time(:nanosecond),
        end_time: System.system_time(:nanosecond) + 50_000_000,
        duration_ns: 50_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET", "service.name" => "api"},
        events: [],
        resource: %{"service.name" => "api"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  describe "compaction on disk" do
    test "compacts raw blocks into openzl (default)" do
      # Flush multiple small batches to create raw blocks
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        TimelessTraces.Buffer.ingest(spans)
        TimelessTraces.flush()
      end

      blocks_dir = Path.join(@data_dir, "blocks")

      # Should have 5 raw files
      raw_before = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_before) == 5

      # Verify stats before compaction
      {:ok, stats_before} = TimelessTraces.stats()
      assert stats_before.total_entries == 25
      assert stats_before.raw_blocks == 5

      # Trigger compaction (threshold is 10, we have 25 entries)
      assert :ok = TimelessTraces.Compactor.compact_now()

      # Raw files should be deleted, replaced by a single openzl file
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      ozl_after = Path.wildcard(Path.join(blocks_dir, "*.ozl"))
      assert raw_after == []
      assert length(ozl_after) == 1

      # All entries still queryable
      {:ok, %TimelessTraces.Result{total: 25}} = TimelessTraces.query([])

      # Stats should show openzl block
      {:ok, stats_after} = TimelessTraces.stats()
      assert stats_after.total_entries == 25
      assert stats_after.raw_blocks == 0
      assert stats_after.openzl_blocks == 1
      assert stats_after.openzl_bytes < stats_before.raw_bytes
    end

    test "compaction preserves trace index" do
      trace_id = "compact-trace-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      # Spread trace spans across multiple flushes
      TimelessTraces.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root",
          start_time: now,
          end_time: now + 100_000_000
        })
      ])

      TimelessTraces.flush()

      TimelessTraces.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "child",
          parent_span_id: "root",
          name: "child",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        })
      ])

      TimelessTraces.flush()

      # Pad with more spans to exceed threshold
      for _ <- 1..10 do
        TimelessTraces.Buffer.ingest([make_span()])
        TimelessTraces.flush()
      end

      # Verify trace works before compaction
      {:ok, pre_spans} = TimelessTraces.trace(trace_id)
      assert length(pre_spans) == 2

      # Compact
      assert :ok = TimelessTraces.Compactor.compact_now()

      # Trace should still work after compaction
      {:ok, post_spans} = TimelessTraces.trace(trace_id)
      assert length(post_spans) == 2
      assert Enum.all?(post_spans, &(&1.trace_id == trace_id))
    end

    test "noop when below threshold" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      assert :noop = TimelessTraces.Compactor.compact_now()
    end
  end

  describe "compaction to zstd on disk (explicit)" do
    setup do
      Application.put_env(:timeless_traces, :compaction_format, :zstd)

      on_exit(fn ->
        Application.delete_env(:timeless_traces, :compaction_format)
      end)

      :ok
    end

    test "compacts raw blocks into zstd when configured" do
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        TimelessTraces.Buffer.ingest(spans)
        TimelessTraces.flush()
      end

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_before = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_before) == 5

      assert :ok = TimelessTraces.Compactor.compact_now()

      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      zst_after = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      assert raw_after == []
      assert length(zst_after) == 1

      {:ok, %TimelessTraces.Result{total: 25}} = TimelessTraces.query([])

      {:ok, stats} = TimelessTraces.stats()
      assert stats.total_entries == 25
      assert stats.raw_blocks == 0
      assert stats.zstd_blocks == 1
      assert stats.zstd_bytes > 0
    end

    test "compaction to zstd preserves trace index" do
      trace_id = "zstd-trace-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      TimelessTraces.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root",
          start_time: now,
          end_time: now + 100_000_000
        })
      ])

      TimelessTraces.flush()

      TimelessTraces.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "child",
          parent_span_id: "root",
          name: "child",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        })
      ])

      TimelessTraces.flush()

      for _ <- 1..10 do
        TimelessTraces.Buffer.ingest([make_span()])
        TimelessTraces.flush()
      end

      {:ok, pre_spans} = TimelessTraces.trace(trace_id)
      assert length(pre_spans) == 2

      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, post_spans} = TimelessTraces.trace(trace_id)
      assert length(post_spans) == 2
      assert Enum.all?(post_spans, &(&1.trace_id == trace_id))
    end
  end

  describe "compaction in memory mode" do
    setup do
      Application.stop(:timeless_traces)
      Application.put_env(:timeless_traces, :storage, :memory)
      Application.put_env(:timeless_traces, :data_dir, "test/tmp/compact_mem_should_not_exist")
      Application.put_env(:timeless_traces, :compaction_threshold, 10)
      Application.ensure_all_started(:timeless_traces)

      on_exit(fn ->
        Application.stop(:timeless_traces)
        Application.put_env(:timeless_traces, :storage, :disk)
      end)

      :ok
    end

    test "compacts in memory mode" do
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        TimelessTraces.Buffer.ingest(spans)
        TimelessTraces.flush()
      end

      {:ok, stats_before} = TimelessTraces.stats()
      assert stats_before.raw_blocks == 5

      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, stats_after} = TimelessTraces.stats()
      assert stats_after.openzl_blocks == 1
      assert stats_after.raw_blocks == 0

      {:ok, %TimelessTraces.Result{total: 25}} = TimelessTraces.query([])
    end
  end

  describe "compaction to zstd in memory mode (explicit)" do
    setup do
      Application.stop(:timeless_traces)
      Application.put_env(:timeless_traces, :storage, :memory)

      Application.put_env(
        :timeless_traces,
        :data_dir,
        "test/tmp/compact_zstd_mem_should_not_exist"
      )

      Application.put_env(:timeless_traces, :compaction_threshold, 10)
      Application.put_env(:timeless_traces, :compaction_format, :zstd)
      Application.ensure_all_started(:timeless_traces)

      on_exit(fn ->
        Application.stop(:timeless_traces)
        Application.put_env(:timeless_traces, :storage, :disk)
        Application.delete_env(:timeless_traces, :compaction_format)
      end)

      :ok
    end

    test "compacts to zstd in memory mode when configured" do
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        TimelessTraces.Buffer.ingest(spans)
        TimelessTraces.flush()
      end

      {:ok, stats_before} = TimelessTraces.stats()
      assert stats_before.raw_blocks == 5

      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, stats_after} = TimelessTraces.stats()
      assert stats_after.zstd_blocks == 1
      assert stats_after.raw_blocks == 0

      {:ok, %TimelessTraces.Result{total: 25}} = TimelessTraces.query([])
    end
  end

  describe "merge compaction on disk" do
    test "merges multiple small compressed blocks into fewer larger ones" do
      # Create 10 separate raw blocks with 5 entries each
      for _ <- 1..10 do
        spans = for _ <- 1..5, do: make_span()
        TimelessTraces.Buffer.ingest(spans)
        TimelessTraces.flush()
      end

      # Compact raw → compressed
      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, stats_before} = TimelessTraces.stats()
      assert stats_before.raw_blocks == 0
      assert stats_before.openzl_blocks >= 1

      # Set merge config: target 200 entries, min 2 blocks
      # Since we compacted 50 entries into 1 block, we need multiple compaction rounds
      # to get multiple small compressed blocks. Instead, flush individual small batches
      # and compact each separately.
      # Actually the first compact produces 1 big block. Let's create more small ones.

      # Create more small raw blocks and compact them individually
      for _ <- 1..4 do
        TimelessTraces.Buffer.ingest([make_span()])
        TimelessTraces.flush()
      end

      # Set threshold to 1 to compact these small blocks
      Application.put_env(:timeless_traces, :compaction_threshold, 1)
      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, stats_mid} = TimelessTraces.stats()
      # Should now have multiple compressed blocks (the big one + the new small one)
      assert stats_mid.raw_blocks == 0
      total_compressed = stats_mid.openzl_blocks + stats_mid.zstd_blocks
      assert total_compressed >= 2

      blocks_before_merge = stats_mid.total_blocks

      # Merge: target large enough to fit all, min_blocks 2
      Application.put_env(:timeless_traces, :merge_compaction_target_size, 500)
      Application.put_env(:timeless_traces, :merge_compaction_min_blocks, 2)

      assert :ok = TimelessTraces.Compactor.merge_now()

      {:ok, stats_after} = TimelessTraces.stats()
      assert stats_after.total_blocks < blocks_before_merge
      assert stats_after.total_entries == 54

      # All entries still queryable
      {:ok, %TimelessTraces.Result{total: 54}} = TimelessTraces.query([])
    end

    test "returns :noop when not enough small blocks" do
      # Create one compressed block via compact cycle
      TimelessTraces.Buffer.ingest(for _ <- 1..15, do: make_span())
      TimelessTraces.flush()

      assert :ok = TimelessTraces.Compactor.compact_now()

      # Only 1 compressed block, min_blocks is 4
      Application.put_env(:timeless_traces, :merge_compaction_min_blocks, 4)
      Application.put_env(:timeless_traces, :merge_compaction_target_size, 500)

      assert :noop = TimelessTraces.Compactor.merge_now()
    end

    test "preserves trace index after merge" do
      trace_id = "merge-trace-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)
      Application.put_env(:timeless_traces, :compaction_threshold, 1)

      # Create trace spans, compact into one compressed block
      TimelessTraces.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root",
          start_time: now,
          end_time: now + 100_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child",
          parent_span_id: "root",
          name: "child",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        })
      ])

      TimelessTraces.flush()
      assert :ok = TimelessTraces.Compactor.compact_now()

      # Create more separate compact cycles to get multiple compressed blocks
      for _ <- 1..3 do
        TimelessTraces.Buffer.ingest([make_span()])
        TimelessTraces.flush()
        assert :ok = TimelessTraces.Compactor.compact_now()
      end

      # Verify trace works before merge
      {:ok, pre_spans} = TimelessTraces.trace(trace_id)
      assert length(pre_spans) == 2

      {:ok, stats_mid} = TimelessTraces.stats()
      assert stats_mid.raw_blocks == 0
      assert stats_mid.total_blocks >= 4

      # Merge compressed blocks
      Application.put_env(:timeless_traces, :merge_compaction_target_size, 500)
      Application.put_env(:timeless_traces, :merge_compaction_min_blocks, 2)
      assert :ok = TimelessTraces.Compactor.merge_now()

      # Trace should still work after merge
      {:ok, post_spans} = TimelessTraces.trace(trace_id)
      assert length(post_spans) == 2
      assert Enum.all?(post_spans, &(&1.trace_id == trace_id))
    end

    test "preserves query after merge" do
      Application.put_env(:timeless_traces, :compaction_threshold, 1)

      # Create multiple small compressed blocks via separate compact cycles
      for _ <- 1..4 do
        TimelessTraces.Buffer.ingest([
          make_span(%{name: "GET /api", status: :ok}),
          make_span(%{name: "POST /submit", status: :error})
        ])

        TimelessTraces.flush()
        assert :ok = TimelessTraces.Compactor.compact_now()
      end

      {:ok, stats_mid} = TimelessTraces.stats()
      assert stats_mid.raw_blocks == 0
      assert stats_mid.total_blocks >= 4

      # Merge compressed blocks
      Application.put_env(:timeless_traces, :merge_compaction_target_size, 500)
      Application.put_env(:timeless_traces, :merge_compaction_min_blocks, 2)
      assert :ok = TimelessTraces.Compactor.merge_now()

      # Filtered query should still work
      {:ok, %TimelessTraces.Result{total: total_errors}} = TimelessTraces.query(status: :error)
      assert total_errors >= 4

      {:ok, %TimelessTraces.Result{total: total}} = TimelessTraces.query([])
      assert total == 8
    end
  end

  describe "merge compaction in memory mode" do
    setup do
      Application.stop(:timeless_traces)
      Application.put_env(:timeless_traces, :storage, :memory)
      Application.put_env(:timeless_traces, :data_dir, "test/tmp/merge_mem_should_not_exist")
      Application.put_env(:timeless_traces, :compaction_threshold, 10)
      Application.ensure_all_started(:timeless_traces)

      on_exit(fn ->
        Application.stop(:timeless_traces)
        Application.put_env(:timeless_traces, :storage, :disk)
      end)

      :ok
    end

    test "works in memory mode" do
      # Create multiple small blocks
      for _ <- 1..6 do
        TimelessTraces.Buffer.ingest(for _ <- 1..5, do: make_span())
        TimelessTraces.flush()
      end

      # Compact raw → compressed
      assert :ok = TimelessTraces.Compactor.compact_now()

      # Create more small blocks and compact again
      for _ <- 1..4 do
        TimelessTraces.Buffer.ingest([make_span()])
        TimelessTraces.flush()
      end

      Application.put_env(:timeless_traces, :compaction_threshold, 1)
      assert :ok = TimelessTraces.Compactor.compact_now()

      {:ok, stats_before} = TimelessTraces.stats()
      assert stats_before.raw_blocks == 0
      blocks_before = stats_before.total_blocks
      assert blocks_before >= 2

      # Merge
      Application.put_env(:timeless_traces, :merge_compaction_target_size, 500)
      Application.put_env(:timeless_traces, :merge_compaction_min_blocks, 2)
      assert :ok = TimelessTraces.Compactor.merge_now()

      {:ok, stats_after} = TimelessTraces.stats()
      assert stats_after.total_blocks < blocks_before
      assert stats_after.total_entries == 34

      {:ok, %TimelessTraces.Result{total: 34}} = TimelessTraces.query([])
    end
  end
end
