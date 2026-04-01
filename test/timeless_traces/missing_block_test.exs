defmodule TimelessTraces.MissingBlockTest do
  use ExUnit.Case, async: false

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "timeless_traces_missing_block_#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(data_dir)

    Application.stop(:timeless_traces)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, data_dir)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.put_env(:timeless_traces, :max_term_index_entries, nil)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      Application.put_env(:timeless_traces, :storage, :disk)
      File.rm_rf!(data_dir)
    end)

    %{data_dir: data_dir}
  end

  defp make_span(overrides) do
    Map.merge(
      %{
        trace_id: "trace-#{System.unique_integer([:positive])}",
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

  test "prunes stale metadata when a block file is missing" do
    trace_id = "missing-trace-#{System.unique_integer([:positive])}"
    spans = [make_span(%{trace_id: trace_id})]

    {:ok, meta} = TimelessTraces.Writer.write_block(spans, TimelessTraces.Config.data_dir(), :raw)

    {terms, trace_rows} = TimelessTraces.Index.precompute(spans)
    :ok = TimelessTraces.Index.index_block(meta, terms, trace_rows)

    assert File.exists?(meta.file_path)
    File.rm!(meta.file_path)

    assert {:ok, []} = TimelessTraces.Index.trace(trace_id)

    TimelessTraces.Index.sync()

    assert TimelessTraces.Index.matching_block_ids([]) == []
  end
end
