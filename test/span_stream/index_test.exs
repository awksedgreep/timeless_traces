defmodule SpanStream.IndexTest do
  use ExUnit.Case, async: false

  setup do
    Application.stop(:span_stream)
    Application.put_env(:span_stream, :storage, :memory)
    Application.put_env(:span_stream, :data_dir, "test/tmp/index_should_not_exist")
    Application.put_env(:span_stream, :flush_interval, 60_000)
    Application.put_env(:span_stream, :max_buffer_size, 10_000)
    Application.put_env(:span_stream, :retention_max_age, nil)
    Application.put_env(:span_stream, :retention_max_size, nil)
    Application.ensure_all_started(:span_stream)

    on_exit(fn ->
      Application.stop(:span_stream)
      Application.put_env(:span_stream, :storage, :disk)
    end)

    :ok
  end

  defp make_span(overrides \\ %{}) do
    Map.merge(
      %{
        trace_id: "trace-1",
        span_id: "span-#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "HTTP GET",
        kind: :server,
        start_time: 1_000_000_000,
        end_time: 1_050_000_000,
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

  describe "index_block and query" do
    test "indexes and retrieves spans" do
      spans = [make_span(), make_span(%{name: "HTTP POST", status: :error})]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, %SpanStream.Result{entries: entries, total: 2}} =
        SpanStream.Index.query([])

      assert length(entries) == 2
    end

    test "queries by name term" do
      spans = [
        make_span(%{name: "HTTP GET"}),
        make_span(%{name: "DB Query"})
      ]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, %SpanStream.Result{entries: entries}} =
        SpanStream.Index.query(name: "HTTP GET")

      assert length(entries) == 1
      assert hd(entries).name == "HTTP GET"
    end

    test "queries by status term" do
      spans = [
        make_span(%{status: :ok}),
        make_span(%{status: :error})
      ]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, %SpanStream.Result{entries: entries}} =
        SpanStream.Index.query(status: :error)

      assert length(entries) == 1
      assert hd(entries).status == :error
    end

    test "queries by kind" do
      spans = [
        make_span(%{kind: :server}),
        make_span(%{kind: :client})
      ]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, %SpanStream.Result{entries: entries}} =
        SpanStream.Index.query(kind: :client)

      assert length(entries) == 1
      assert hd(entries).kind == :client
    end
  end

  describe "trace/1" do
    test "retrieves all spans for a trace" do
      spans = [
        make_span(%{trace_id: "trace-abc", span_id: "root", parent_span_id: nil, name: "root"}),
        make_span(%{
          trace_id: "trace-abc",
          span_id: "child1",
          parent_span_id: "root",
          name: "child1"
        }),
        make_span(%{
          trace_id: "trace-abc",
          span_id: "child2",
          parent_span_id: "root",
          name: "child2"
        }),
        make_span(%{trace_id: "trace-other", span_id: "other", name: "other"})
      ]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, trace_spans} = SpanStream.Index.trace("trace-abc")
      assert length(trace_spans) == 3
      assert Enum.all?(trace_spans, &(&1.trace_id == "trace-abc"))
    end

    test "returns empty list for unknown trace" do
      {:ok, spans} = SpanStream.Index.trace("nonexistent-trace")
      assert spans == []
    end
  end

  describe "stats/0" do
    test "returns aggregate stats" do
      {:ok, stats} = SpanStream.Index.stats()
      assert stats.total_blocks == 0

      spans = [make_span()]
      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, stats} = SpanStream.Index.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.total_bytes > 0
    end
  end

  describe "term extraction" do
    test "indexes service.name from attributes" do
      spans = [
        make_span(%{attributes: %{"service.name" => "payments"}})
      ]

      {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      :ok = SpanStream.Index.index_block(meta, spans)

      {:ok, %SpanStream.Result{entries: entries}} =
        SpanStream.Index.query(service: "payments")

      assert length(entries) == 1
    end
  end
end
