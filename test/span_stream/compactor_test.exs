defmodule SpanStream.CompactorTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/compactor_#{System.unique_integer([:positive])}"

  setup do
    Application.stop(:span_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:span_stream, :storage, :disk)
    Application.put_env(:span_stream, :data_dir, @data_dir)
    Application.put_env(:span_stream, :flush_interval, 60_000)
    Application.put_env(:span_stream, :max_buffer_size, 10_000)
    Application.put_env(:span_stream, :retention_max_age, nil)
    Application.put_env(:span_stream, :retention_max_size, nil)
    Application.put_env(:span_stream, :compaction_threshold, 10)
    Application.put_env(:span_stream, :compaction_max_raw_age, 60)
    Application.ensure_all_started(:span_stream)

    on_exit(fn ->
      Application.stop(:span_stream)
      File.rm_rf!(@data_dir)
      Application.put_env(:span_stream, :storage, :disk)
      Application.put_env(:span_stream, :compaction_threshold, 500)
      Application.put_env(:span_stream, :compaction_max_raw_age, 60)
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
    test "compacts raw blocks into zstd" do
      # Flush multiple small batches to create raw blocks
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        SpanStream.Buffer.ingest(spans)
        SpanStream.flush()
      end

      blocks_dir = Path.join(@data_dir, "blocks")

      # Should have 5 raw files
      raw_before = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_before) == 5

      # Verify stats before compaction
      {:ok, stats_before} = SpanStream.stats()
      assert stats_before.total_entries == 25
      assert stats_before.raw_blocks == 5

      # Trigger compaction (threshold is 10, we have 25 entries)
      assert :ok = SpanStream.Compactor.compact_now()

      # Raw files should be deleted, replaced by a single zstd file
      raw_after = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      zst_after = Path.wildcard(Path.join(blocks_dir, "*.zst"))
      assert raw_after == []
      assert length(zst_after) == 1

      # All entries still queryable
      {:ok, %SpanStream.Result{total: 25}} = SpanStream.query([])

      # Stats should show zstd block
      {:ok, stats_after} = SpanStream.stats()
      assert stats_after.total_entries == 25
      assert stats_after.raw_blocks == 0
      assert stats_after.zstd_blocks == 1
      assert stats_after.zstd_bytes < stats_before.raw_bytes
    end

    test "compaction preserves trace index" do
      trace_id = "compact-trace-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      # Spread trace spans across multiple flushes
      SpanStream.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root",
          start_time: now,
          end_time: now + 100_000_000
        })
      ])

      SpanStream.flush()

      SpanStream.Buffer.ingest([
        make_span(%{
          trace_id: trace_id,
          span_id: "child",
          parent_span_id: "root",
          name: "child",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        })
      ])

      SpanStream.flush()

      # Pad with more spans to exceed threshold
      for _ <- 1..10 do
        SpanStream.Buffer.ingest([make_span()])
        SpanStream.flush()
      end

      # Verify trace works before compaction
      {:ok, pre_spans} = SpanStream.trace(trace_id)
      assert length(pre_spans) == 2

      # Compact
      assert :ok = SpanStream.Compactor.compact_now()

      # Trace should still work after compaction
      {:ok, post_spans} = SpanStream.trace(trace_id)
      assert length(post_spans) == 2
      assert Enum.all?(post_spans, &(&1.trace_id == trace_id))
    end

    test "noop when below threshold" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      assert :noop = SpanStream.Compactor.compact_now()
    end
  end

  describe "compaction in memory mode" do
    setup do
      Application.stop(:span_stream)
      Application.put_env(:span_stream, :storage, :memory)
      Application.put_env(:span_stream, :data_dir, "test/tmp/compact_mem_should_not_exist")
      Application.put_env(:span_stream, :compaction_threshold, 10)
      Application.ensure_all_started(:span_stream)

      on_exit(fn ->
        Application.stop(:span_stream)
        Application.put_env(:span_stream, :storage, :disk)
      end)

      :ok
    end

    test "compacts in memory mode" do
      for _ <- 1..5 do
        spans = for _ <- 1..5, do: make_span()
        SpanStream.Buffer.ingest(spans)
        SpanStream.flush()
      end

      {:ok, stats_before} = SpanStream.stats()
      assert stats_before.raw_blocks == 5

      assert :ok = SpanStream.Compactor.compact_now()

      {:ok, stats_after} = SpanStream.stats()
      assert stats_after.zstd_blocks == 1
      assert stats_after.raw_blocks == 0

      {:ok, %SpanStream.Result{total: 25}} = SpanStream.query([])
    end
  end
end
