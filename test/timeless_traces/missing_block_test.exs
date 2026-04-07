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

  test "query skips missing blocks and preserves surviving page slices" do
    service_name = "missing-page-#{System.unique_integer([:positive])}"

    spans =
      for idx <- 1..6 do
        make_span(%{
          name: "missing-page-#{idx}",
          start_time: idx * 1_000,
          end_time: idx * 1_000 + 100,
          duration_ns: 100,
          attributes: %{"service.name" => service_name, "host.name" => "missing-host"}
        })
      end

    {first_half, second_half} = Enum.split(spans, 3)

    {:ok, meta1} =
      TimelessTraces.Writer.write_block(first_half, TimelessTraces.Config.data_dir(), :raw)

    {terms1, trace_rows1} = TimelessTraces.Index.precompute(first_half)
    :ok = TimelessTraces.Index.index_block(meta1, terms1, trace_rows1)

    {:ok, meta2} =
      TimelessTraces.Writer.write_block(second_half, TimelessTraces.Config.data_dir(), :raw)

    {terms2, trace_rows2} = TimelessTraces.Index.precompute(second_half)
    :ok = TimelessTraces.Index.index_block(meta2, terms2, trace_rows2)
    TimelessTraces.Index.sync()

    File.rm!(meta1.file_path)

    filters = [order: :asc, attributes: %{"service.name" => service_name}]

    {:ok, %TimelessTraces.Result{entries: exact_entries, total: 3}} =
      TimelessTraces.query([limit: 3] ++ filters)

    {:ok, %TimelessTraces.Result{entries: fast_entries, total: 3, has_more: false}} =
      TimelessTraces.query([limit: 3, count_total: false] ++ filters)

    assert Enum.map(exact_entries, & &1.name) == [
             "missing-page-4",
             "missing-page-5",
             "missing-page-6"
           ]

    assert Enum.map(fast_entries, & &1.name) == Enum.map(exact_entries, & &1.name)
  end
end
