defmodule SpanStream.BufferTest do
  use ExUnit.Case, async: false

  setup do
    Application.stop(:span_stream)
    Application.put_env(:span_stream, :storage, :memory)
    Application.put_env(:span_stream, :data_dir, "test/tmp/buffer_should_not_exist")
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
        trace_id: "trace-#{System.unique_integer([:positive])}",
        span_id: "span-#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "test-op",
        kind: :internal,
        start_time: System.system_time(:nanosecond),
        end_time: System.system_time(:nanosecond) + 1_000_000,
        duration_ns: 1_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{},
        events: [],
        resource: %{},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  describe "ingest and flush" do
    test "ingests spans and flushes to index" do
      spans = [make_span(), make_span(), make_span()]
      SpanStream.Buffer.ingest(spans)
      SpanStream.Buffer.flush()

      {:ok, %SpanStream.Result{total: 3}} = SpanStream.Index.query([])
    end

    test "multiple ingests accumulate" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.Buffer.flush()

      {:ok, %SpanStream.Result{total: 3}} = SpanStream.Index.query([])
    end

    test "empty flush is safe" do
      assert :ok = SpanStream.Buffer.flush()
    end
  end

  describe "auto-flush on buffer size" do
    test "flushes when buffer reaches max size" do
      Application.put_env(:span_stream, :max_buffer_size, 5)

      spans = for _ <- 1..10, do: make_span()
      SpanStream.Buffer.ingest(spans)

      # Give the GenServer time to process the auto-flush
      Process.sleep(100)

      {:ok, %SpanStream.Result{total: total}} = SpanStream.Index.query([])
      assert total == 10
    end
  end

  describe "subscription" do
    test "subscribers receive spans on ingest" do
      SpanStream.subscribe()
      span = make_span(%{name: "subscribed-op"})
      SpanStream.Buffer.ingest([span])

      assert_receive {:span_stream, :span, %SpanStream.Span{name: "subscribed-op"}}, 1000
      SpanStream.unsubscribe()
    end
  end
end
