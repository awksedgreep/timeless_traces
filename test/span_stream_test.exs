defmodule SpanStreamTest do
  use ExUnit.Case, async: false

  setup do
    Application.stop(:span_stream)
    Application.put_env(:span_stream, :storage, :memory)
    Application.put_env(:span_stream, :data_dir, "test/tmp/e2e_should_not_exist")
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

  describe "full pipeline" do
    test "ingest → flush → query returns spans" do
      spans = [
        make_span(%{name: "GET /users", status: :ok}),
        make_span(%{name: "POST /users", status: :error}),
        make_span(%{name: "GET /health", status: :ok})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: all, total: 3}} = SpanStream.query([])
      assert length(all) == 3
      assert Enum.all?(all, &match?(%SpanStream.Span{}, &1))
    end

    test "query by status filter" do
      spans = [
        make_span(%{status: :ok}),
        make_span(%{status: :error}),
        make_span(%{status: :ok})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: errors, total: 1}} =
        SpanStream.query(status: :error)

      assert length(errors) == 1
      assert hd(errors).status == :error
    end

    test "query by kind filter" do
      spans = [
        make_span(%{kind: :server}),
        make_span(%{kind: :client}),
        make_span(%{kind: :internal})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: servers}} = SpanStream.query(kind: :server)
      assert length(servers) == 1
      assert hd(servers).kind == :server
    end

    test "query with duration filters" do
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          start_time: now,
          end_time: now + 10_000_000,
          duration_ns: 10_000_000,
          name: "fast"
        }),
        make_span(%{
          start_time: now + 1,
          end_time: now + 1 + 200_000_000,
          duration_ns: 200_000_000,
          name: "slow"
        })
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: slow}} =
        SpanStream.query(min_duration: 100_000_000)

      assert length(slow) == 1
      assert hd(slow).name == "slow"
    end

    test "trace/1 retrieves full trace" do
      trace_id = "trace-full-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root-span",
          start_time: now,
          end_time: now + 100_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child-1",
          parent_span_id: "root",
          name: "child-span-1",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child-2",
          parent_span_id: "root",
          name: "child-span-2",
          start_time: now + 20_000_000,
          end_time: now + 80_000_000
        }),
        make_span(%{name: "unrelated"})
      ]

      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, trace_spans} = SpanStream.trace(trace_id)
      assert length(trace_spans) == 3
      assert Enum.all?(trace_spans, &(&1.trace_id == trace_id))

      # Should be sorted by start_time
      times = Enum.map(trace_spans, & &1.start_time)
      assert times == Enum.sort(times)
    end

    test "pagination works" do
      spans = for i <- 1..20, do: make_span(%{name: "span-#{i}"})
      SpanStream.Buffer.ingest(spans)
      SpanStream.flush()

      {:ok, %SpanStream.Result{entries: page1, total: 20, limit: 5}} =
        SpanStream.query(limit: 5)

      assert length(page1) == 5

      {:ok, %SpanStream.Result{entries: page2, offset: 5}} =
        SpanStream.query(limit: 5, offset: 5)

      assert length(page2) == 5
      assert hd(page1).span_id != hd(page2).span_id
    end

    test "stats/0 returns aggregate data" do
      {:ok, stats} = SpanStream.stats()
      assert stats.total_blocks == 0

      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      {:ok, stats} = SpanStream.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.total_bytes > 0
    end

    test "subscribe/unsubscribe for live spans" do
      SpanStream.subscribe()

      span = make_span(%{name: "live-span"})
      SpanStream.Buffer.ingest([span])

      assert_receive {:span_stream, :span, %SpanStream.Span{name: "live-span"}}, 1000

      SpanStream.unsubscribe()
    end

    test "multiple flushes accumulate" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      {:ok, %SpanStream.Result{total: 3}} = SpanStream.query([])
    end

    test "no files created in memory mode" do
      SpanStream.Buffer.ingest([make_span()])
      SpanStream.flush()

      refute File.exists?("test/tmp/e2e_should_not_exist")
    end
  end
end
