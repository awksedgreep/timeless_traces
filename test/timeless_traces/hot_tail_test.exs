defmodule TimelessTraces.HotTailTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/hot_tail"

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, @data_dir)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  defp span(trace_id, name, start_ns) do
    %{
      trace_id: trace_id,
      span_id: "s-#{System.unique_integer([:positive])}",
      parent_span_id: nil,
      name: name,
      kind: :server,
      start_time: start_ns,
      end_time: start_ns + 1_000_000,
      duration_ns: 1_000_000,
      status: :ok,
      status_message: nil,
      attributes: %{"service.name" => "tail-test"},
      events: [],
      resource: %{"service.name" => "tail-test"},
      instrumentation_scope: nil
    }
  end

  test "spans are queryable before any flush" do
    now = System.os_time(:nanosecond)
    spans = for i <- 1..40, do: span("t-#{i}", "op.fresh", now - i * 1_000)

    TimelessTraces.Buffer.ingest(spans)

    {:ok, %{entries: results, total: total}} =
      TimelessTraces.query(service: "tail-test", count_total: true)

    assert total == 40
    assert length(results) == 40
  end

  test "no duplicates once spans are flushed and indexed" do
    now = System.os_time(:nanosecond)
    spans = for i <- 1..60, do: span("t-#{i}", "op.flushed", now - i * 1_000)

    TimelessTraces.Buffer.ingest(spans)
    :ok = TimelessTraces.flush()
    TimelessTraces.Index.sync()

    {:ok, %{total: total}} = TimelessTraces.query(service: "tail-test", count_total: true)
    assert total == 60
  end

  test "trace lookup merges tail and disk spans without duplicates" do
    now = System.os_time(:nanosecond)

    # First half of the trace flushed to disk
    first = for i <- 1..3, do: span("t-straddle", "op.early", now - 60_000_000_000 + i)
    TimelessTraces.Buffer.ingest(first)
    :ok = TimelessTraces.flush()
    TimelessTraces.Index.sync()

    # Second half still tail-only
    second = for i <- 1..2, do: span("t-straddle", "op.late", now - i * 1_000)
    TimelessTraces.Buffer.ingest(second)

    {:ok, spans} = TimelessTraces.trace("t-straddle")
    assert length(spans) == 5
    assert spans |> Enum.map(& &1.span_id) |> Enum.uniq() |> length() == 5
  end

  test "cap eviction removes companion trace-index rows" do
    Application.put_env(:timeless_traces, :hot_tail_max_entries, 2)
    on_exit(fn -> Application.delete_env(:timeless_traces, :hot_tail_max_entries) end)
    now = System.os_time(:nanosecond)
    spans = for i <- 1..5, do: span("cap-#{i}", "op.cap", now + i)

    TimelessTraces.HotTail.insert_many(spans)
    send(TimelessTraces.HotTail, :sweep)
    _ = :sys.get_state(TimelessTraces.HotTail)

    assert :ets.info(TimelessTraces.HotTail, :size) == 2
    assert :ets.info(TimelessTraces.HotTail.ByTrace, :size) == 2
    assert TimelessTraces.HotTail.trace_spans("cap-1") == []
    assert length(TimelessTraces.HotTail.trace_spans("cap-5")) == 1
  end
end
