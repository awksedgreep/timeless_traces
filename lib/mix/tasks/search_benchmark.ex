defmodule Mix.Tasks.TimelessTraces.SearchBenchmark do
  @moduledoc "Benchmark query throughput across different filter patterns"
  use Mix.Task

  @shortdoc "Benchmark span search/query performance"

  @impl true
  def run(_args) do
    data_dir = "search_bench_#{System.unique_integer([:positive])}"
    blocks_dir = Path.join(data_dir, "blocks")
    File.mkdir_p!(blocks_dir)

    Application.put_env(:timeless_traces, :data_dir, data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :compaction_interval, 600_000)
    Mix.Task.run("app.start")

    IO.puts("=== TimelessTraces Search Benchmark ===\n")

    # Generate and index data
    span_count = 200_000
    IO.puts("Generating and indexing #{fmt_number(span_count)} spans...")
    {setup_us, trace_ids} = :timer.tc(fn -> seed_data(data_dir, span_count) end)
    IO.puts("Setup: #{fmt_ms(setup_us)}\n")

    {:ok, stats} = TimelessTraces.Index.stats()
    IO.puts("Indexed: #{stats.total_blocks} blocks, #{fmt_number(stats.total_entries)} spans\n")

    # Benchmark queries
    queries = [
      {"status=error", [status: :error, limit: 1000]},
      {"kind=server", [kind: :server, limit: 1000]},
      {"service=api-gateway", [service: "api-gateway", limit: 1000]},
      {"name contains 'HTTP'", [name: "HTTP", limit: 1000]},
      {"all spans (limit 1000)", [limit: 1000]},
      {"last 6h", [since: System.system_time(:nanosecond) - 6 * 3_600_000_000_000, limit: 1000]}
    ]

    IO.puts("--- Query Benchmarks (3 iterations each) ---")

    results =
      for {label, filters} <- queries do
        times =
          for _ <- 1..3 do
            {us, {:ok, result}} = :timer.tc(fn -> TimelessTraces.Index.query(filters) end)
            {us, result.total}
          end

        avg_us = times |> Enum.map(&elem(&1, 0)) |> Enum.sum() |> div(3)
        {_, total} = hd(times)

        IO.puts(
          "  #{String.pad_trailing(label, 30)} #{fmt_ms(avg_us)} avg (#{fmt_number(total)} matches)"
        )

        {label, avg_us}
      end

    # Benchmark trace lookup
    IO.puts("\n--- Trace Lookup Benchmark ---")
    sample_traces = Enum.take_random(trace_ids, min(10, length(trace_ids)))

    trace_times =
      for trace_id <- sample_traces do
        {us, {:ok, spans}} = :timer.tc(fn -> TimelessTraces.Index.trace(trace_id) end)
        {us, length(spans)}
      end

    avg_trace_us =
      trace_times |> Enum.map(&elem(&1, 0)) |> Enum.sum() |> div(max(length(trace_times), 1))

    avg_spans =
      trace_times |> Enum.map(&elem(&1, 1)) |> Enum.sum() |> div(max(length(trace_times), 1))

    IO.puts(
      "  Trace lookup (#{length(sample_traces)} traces): #{fmt_ms(avg_trace_us)} avg (#{avg_spans} spans/trace avg)"
    )

    # Summary
    IO.puts("\n=== Summary ===")

    for {label, us} <- results do
      IO.puts("  #{String.pad_trailing(label, 30)} #{fmt_ms(us)}")
    end

    IO.puts("  #{String.pad_trailing("trace lookup", 30)} #{fmt_ms(avg_trace_us)}")

    Application.stop(:timeless_traces)
    File.rm_rf!(data_dir)
  end

  defp seed_data(data_dir, count) do
    base_ts = System.system_time(:nanosecond) - 86_400_000_000_000
    services = ~w(api-gateway user-service order-service payment-service inventory-service)
    names = ~w(HTTP\ GET HTTP\ POST db.query cache.get queue.publish grpc.call)
    kinds = [:server, :client, :internal, :producer, :consumer]
    methods = ~w(GET GET GET POST PUT DELETE)
    routes = ~w(/api/users /api/orders /api/products /api/checkout /health)

    # Generate spans grouped by trace (3-8 spans per trace)
    {spans, trace_ids, _} =
      Enum.reduce(1..count, {[], [], 0}, fn _i, {acc, tids, idx} ->
        spans_in_trace = Enum.random(3..8)
        trace_id = random_hex(16)
        service = Enum.random(services)
        ts = base_ts + idx * 1_000_000

        trace_spans =
          Enum.map(1..spans_in_trace, fn j ->
            span_ts = ts + j * 100_000
            duration = :rand.uniform(500_000_000)
            status = Enum.random([:ok, :ok, :ok, :ok, :error, :unset])

            %{
              trace_id: trace_id,
              span_id: random_hex(8),
              parent_span_id: if(j == 1, do: nil, else: random_hex(8)),
              name: Enum.random(names),
              kind: Enum.random(kinds),
              start_time: span_ts,
              end_time: span_ts + duration,
              status: status,
              status_message: if(status == :error, do: "internal error", else: nil),
              attributes: %{
                "service.name" => service,
                "http.method" => Enum.random(methods),
                "http.route" => Enum.random(routes),
                "http.status_code" => "#{Enum.random([200, 200, 200, 201, 400, 404, 500])}"
              },
              events: [],
              resource: %{"service.name" => service}
            }
          end)

        {trace_spans ++ acc, [trace_id | tids], idx + spans_in_trace}
      end)

    # Write and index in blocks of 1000
    spans
    |> Enum.reverse()
    |> Enum.chunk_every(1000)
    |> Enum.each(fn chunk ->
      case TimelessTraces.Writer.write_block(chunk, data_dir, :raw) do
        {:ok, meta} -> TimelessTraces.Index.index_block(meta, chunk)
        _ -> :ok
      end
    end)

    Enum.uniq(trace_ids)
  end

  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"

  defp fmt_ms(us) when us < 1_000, do: "#{us}us"
  defp fmt_ms(us) when us < 1_000_000, do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"
  defp fmt_ms(us), do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"
end
