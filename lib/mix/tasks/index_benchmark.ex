defmodule Mix.Tasks.TimelessTraces.IndexBenchmark do
  @moduledoc "Benchmark pure ETS index operations in isolation (no disk I/O)"
  use Mix.Task

  @shortdoc "Benchmark index ETS operations with in-memory storage"

  @block_count 2_000
  @spans_per_block 100
  @iterations 5
  @destructive_iterations 3

  @services ~w(api-gateway user-service order-service payment-service inventory-service)
  @names ~w(HTTP\ GET HTTP\ POST db.query cache.get queue.publish grpc.call)
  @kinds [:server, :client, :internal, :producer, :consumer]

  @impl true
  def run(_args) do
    Application.put_env(:timeless_traces, :storage, :memory)
    Application.put_env(:timeless_traces, :compaction_interval, 600_000)
    Mix.Task.run("app.start")

    total_spans = @block_count * @spans_per_block

    IO.puts("=== TimelessTraces Index Benchmark ===\n")

    IO.puts(
      "Seeding #{fmt_number(@block_count)} blocks (#{fmt_number(total_spans)} spans, ~200K term index entries)..."
    )

    {setup_us, _} = :timer.tc(fn -> seed_blocks() end)
    IO.puts("Setup: #{fmt_ms(setup_us)}\n")

    {:ok, stats} = TimelessTraces.Index.stats()
    term_count = :ets.info(:timeless_traces_term_index, :size)

    IO.puts("Total blocks: #{fmt_number(stats.total_blocks)}")
    IO.puts("Total entries: #{fmt_number(stats.total_entries)}")
    IO.puts("Total term index entries: #{fmt_number(term_count)}\n")

    six_hours_ago = System.system_time(:nanosecond) - 6 * 3_600_000_000_000

    benchmarks = [
      {"stats()", fn -> TimelessTraces.Index.stats() end},
      {"raw_block_ids()", fn -> TimelessTraces.Index.raw_block_ids() end},
      {"matching_block_ids (no filter)",
       fn -> TimelessTraces.Index.matching_block_ids(limit: 1000) end},
      {"matching_block_ids (term filter)",
       fn -> TimelessTraces.Index.matching_block_ids(service: "api-gateway", limit: 1000) end},
      {"matching_block_ids (time range)",
       fn -> TimelessTraces.Index.matching_block_ids(since: six_hours_ago, limit: 1000) end},
      {"matching_block_ids (term + time)",
       fn ->
         TimelessTraces.Index.matching_block_ids(
           service: "api-gateway",
           since: six_hours_ago,
           limit: 1000
         )
       end},
      {"distinct_services()", fn -> TimelessTraces.Index.distinct_services() end},
      {"distinct_operations(svc)",
       fn -> TimelessTraces.Index.distinct_operations("api-gateway") end}
    ]

    IO.puts("--- Index Operations (#{@iterations} iterations) ---")

    IO.puts(
      String.pad_trailing("Operation", 38) <>
        String.pad_leading("Median", 10) <>
        String.pad_leading("Min", 10) <>
        String.pad_leading("Max", 10)
    )

    IO.puts(String.duplicate("-", 68))

    for {label, fun} <- benchmarks do
      {median, min_l, max_l} = bench(fun)

      IO.puts(
        String.pad_trailing(label, 38) <>
          String.pad_leading(fmt_ms(median), 10) <>
          String.pad_leading(fmt_ms(min_l), 10) <>
          String.pad_leading(fmt_ms(max_l), 10)
      )
    end

    # Destructive operations — restart app + re-seed before each iteration
    IO.puts(
      "\n--- Destructive Operations (#{@destructive_iterations} iterations, re-seed each) ---"
    )

    IO.puts(
      String.pad_trailing("Operation", 38) <>
        String.pad_leading("Median", 10) <>
        String.pad_leading("Min", 10) <>
        String.pad_leading("Max", 10)
    )

    IO.puts(String.duplicate("-", 68))

    destructive_ops = [
      {"delete_blocks_over_size",
       fn ->
         {:ok, s} = TimelessTraces.Index.stats()
         TimelessTraces.Index.delete_blocks_over_size(div(s.total_bytes, 2))
       end},
      {"delete_by_term_limit",
       fn ->
         half_terms = div(:ets.info(:timeless_traces_term_index, :size), 2)
         TimelessTraces.Index.delete_oldest_blocks_until_term_limit(half_terms)
       end}
    ]

    for {label, fun} <- destructive_ops do
      times =
        for _ <- 1..@destructive_iterations do
          reseed()
          {us, _} = :timer.tc(fun)
          us
        end

      sorted = Enum.sort(times)
      median = Enum.at(sorted, div(@destructive_iterations, 2))
      min_l = hd(sorted)
      max_l = List.last(sorted)

      IO.puts(
        String.pad_trailing(label, 38) <>
          String.pad_leading(fmt_ms(median), 10) <>
          String.pad_leading(fmt_ms(min_l), 10) <>
          String.pad_leading(fmt_ms(max_l), 10)
      )
    end

    IO.puts("\n=== Summary ===")
    IO.puts("Total term index entries: #{fmt_number(term_count)}")
    IO.puts("Total blocks: #{fmt_number(stats.total_blocks)}")
    IO.puts("Total spans: #{fmt_number(stats.total_entries)}")

    Application.stop(:timeless_traces)
  end

  defp reseed do
    Application.stop(:timeless_traces)
    Application.ensure_all_started(:timeless_traces)
    seed_blocks()
  end

  defp seed_blocks do
    base_ts = System.system_time(:nanosecond) - 86_400_000_000_000

    for i <- 1..@block_count do
      chunk = generate_chunk(base_ts, i)

      case TimelessTraces.Writer.write_block(chunk, :memory, :raw) do
        {:ok, meta} ->
          {terms, trace_rows} = TimelessTraces.Index.precompute(chunk)
          TimelessTraces.Index.index_block(meta, terms, trace_rows)

        _ ->
          :ok
      end
    end
  end

  defp generate_chunk(base_ts, block_idx) do
    for j <- 1..@spans_per_block do
      ts = base_ts + (block_idx * @spans_per_block + j) * 1_000_000
      duration = :rand.uniform(500_000_000)
      service = Enum.at(@services, rem(block_idx + j, length(@services)))
      status = if(rem(j, 20) == 0, do: :error, else: :ok)

      %{
        trace_id: random_hex(16),
        span_id: random_hex(8),
        parent_span_id: nil,
        name: Enum.at(@names, rem(j, length(@names))),
        kind: Enum.at(@kinds, rem(j, length(@kinds))),
        start_time: ts,
        end_time: ts + duration,
        status: status,
        status_message: if(status == :error, do: "internal error", else: nil),
        attributes: %{
          "service.name" => service,
          "http.method" => Enum.random(~w(GET POST PUT DELETE)),
          "http.route" => Enum.random(~w(/api/users /api/orders /api/products /health)),
          "http.status_code" => "#{Enum.random([200, 201, 400, 404, 500])}"
        },
        events: [],
        resource: %{"service.name" => service}
      }
    end
  end

  defp bench(fun) do
    times = for _ <- 1..@iterations, do: elem(:timer.tc(fun), 0)
    sorted = Enum.sort(times)
    {Enum.at(sorted, div(@iterations, 2)), hd(sorted), List.last(sorted)}
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
