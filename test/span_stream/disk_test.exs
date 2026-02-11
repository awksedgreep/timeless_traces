defmodule SpanStream.DiskTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/disk_#{System.unique_integer([:positive])}"

  setup do
    Application.stop(:span_stream)
    File.rm_rf!(@data_dir)
    Application.put_env(:span_stream, :storage, :disk)
    Application.put_env(:span_stream, :data_dir, @data_dir)
    Application.put_env(:span_stream, :flush_interval, 60_000)
    Application.put_env(:span_stream, :max_buffer_size, 10_000)
    Application.put_env(:span_stream, :retention_max_age, nil)
    Application.put_env(:span_stream, :retention_max_size, nil)
    Application.ensure_all_started(:span_stream)

    on_exit(fn ->
      Application.stop(:span_stream)
      File.rm_rf!(@data_dir)
      Application.put_env(:span_stream, :storage, :disk)
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

  describe "disk storage" do
    test "creates blocks directory" do
      assert File.dir?(Path.join(@data_dir, "blocks"))
    end

    test "creates index.db" do
      assert File.exists?(Path.join(@data_dir, "index.db"))
    end

    test "ingest, flush, and query with disk storage" do
      spans = [
        make_span(%{name: "GET /users", status: :ok}),
        make_span(%{name: "POST /users", status: :error}),
        make_span(%{name: "GET /health"})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: all, total: 3}} = SpanStream.query([])
      assert length(all) == 3
      assert Enum.all?(all, &match?(%SpanStream.Span{}, &1))
    end

    test "writes raw block files to disk" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) == 1

      data = File.read!(hd(raw_files))
      assert {:ok, entries} = SpanStream.Writer.decompress_block(data, :raw)
      assert length(entries) == 1
    end

    test "trace lookup works on disk" do
      trace_id = "disk-trace-#{System.unique_integer([:positive])}"

      spans = [
        make_span(%{trace_id: trace_id, span_id: "root", parent_span_id: nil, name: "root"}),
        make_span(%{trace_id: trace_id, span_id: "child", parent_span_id: "root", name: "child"})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, trace_spans} = SpanStream.trace(trace_id)
      assert length(trace_spans) == 2
    end

    test "stats reflect disk usage" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      {:ok, stats} = SpanStream.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.index_size > 0
      assert stats.raw_blocks == 1
    end

    test "multiple flushes create multiple block files" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) == 2

      {:ok, %SpanStream.Result{total: 2}} = SpanStream.query([])
    end

    test "filtering works on disk" do
      spans = [
        make_span(%{status: :ok}),
        make_span(%{status: :error})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: errors}} = SpanStream.query(status: :error)
      assert length(errors) == 1
      assert hd(errors).status == :error
    end
  end
end
