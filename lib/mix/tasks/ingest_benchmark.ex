defmodule Mix.Tasks.TimelessTraces.IngestBenchmark do
  @moduledoc "Benchmark ingestion throughput through the full pipeline"
  use Mix.Task

  @shortdoc "Benchmark span ingestion throughput (Buffer → Writer → Index)"
  @shard_scenarios [1, 2, 4, 8]

  @impl true
  def run(_args) do
    data_dir = "ingest_bench_#{System.unique_integer([:positive])}"
    blocks_dir = Path.join(data_dir, "blocks")
    File.mkdir_p!(blocks_dir)

    configure_app(data_dir)
    Mix.Task.run("app.start")

    IO.puts("=== TimelessTraces Ingestion Benchmark ===\n")

    # Pre-generate spans to exclude generation time
    span_count = 500_000
    IO.puts("Pre-generating #{fmt_number(span_count)} spans...")
    spans = generate_spans(span_count)
    IO.puts("Done.\n")

    # Phase 1: Writer-only throughput (serialization + disk I/O)
    IO.puts("--- Phase 1: Writer-only (no indexing) ---")
    writer_dir = Path.join(data_dir, "writer_bench")
    File.mkdir_p!(Path.join(writer_dir, "blocks"))

    {writer_us, writer_blocks} =
      :timer.tc(fn ->
        spans
        |> Enum.chunk_every(1000)
        |> Enum.reduce(0, fn chunk, count ->
          case TimelessTraces.Writer.write_block(chunk, writer_dir, :raw) do
            {:ok, _} -> count + 1
            _ -> count
          end
        end)
      end)

    writer_eps = span_count / (writer_us / 1_000_000)
    IO.puts("  #{writer_blocks} blocks in #{fmt_ms(writer_us)}")
    IO.puts("  Throughput: #{fmt_number(round(writer_eps))} spans/sec\n")

    # Phase 2: Writer + Index (sequential, sync indexing)
    IO.puts("--- Phase 2: Writer + Index (sync) ---")
    idx_dir = Path.join(data_dir, "idx_bench")
    File.mkdir_p!(Path.join(idx_dir, "blocks"))
    restart_traces(idx_dir)

    {idx_us, idx_blocks} =
      :timer.tc(fn ->
        spans
        |> Enum.chunk_every(1000)
        |> Enum.reduce(0, fn chunk, count ->
          case TimelessTraces.Writer.write_block(chunk, idx_dir, :raw) do
            {:ok, meta} ->
              {terms, trace_rows} = TimelessTraces.Index.precompute(chunk)
              TimelessTraces.Index.index_block(meta, terms, trace_rows)
              count + 1

            _ ->
              count
          end
        end)
      end)

    idx_eps = span_count / (idx_us / 1_000_000)
    IO.puts("  #{idx_blocks} blocks in #{fmt_ms(idx_us)}")
    IO.puts("  Throughput: #{fmt_number(round(idx_eps))} spans/sec")
    overhead = (idx_us - writer_us) / 1000
    IO.puts("  Index overhead: #{:erlang.float_to_binary(overhead, decimals: 1)}ms total\n")

    # Phase 3: Full pipeline (Buffer.ingest → flush → Writer → async Index)
    IO.puts("--- Phase 3: Full pipeline (Buffer.ingest → Writer → Index) ---")
    pipe_dir = Path.join(data_dir, "pipe_bench")
    restart_traces(pipe_dir)

    {pipe_us, _} =
      :timer.tc(fn ->
        spans
        |> Enum.chunk_every(100)
        |> Enum.each(fn batch ->
          TimelessTraces.Buffer.ingest(batch)
        end)

        # Flush remaining buffer
        TimelessTraces.Buffer.flush()
        # Drain Index mailbox and publish cache
        TimelessTraces.Index.sync()
      end)

    pipe_eps = span_count / (pipe_us / 1_000_000)
    {:ok, stats} = TimelessTraces.Index.stats()

    IO.puts("  #{stats.total_blocks} blocks, #{fmt_number(stats.total_entries)} spans indexed")
    IO.puts("  Wall time: #{fmt_ms(pipe_us)}")
    IO.puts("  Throughput: #{fmt_number(round(pipe_eps))} spans/sec\n")

    IO.puts("--- Phase 3b: Full pipeline by shard count ---")

    shard_results =
      Enum.map(@shard_scenarios, fn shard_count ->
        shard_dir = Path.join(data_dir, "pipe_shards_#{shard_count}")
        restart_traces(shard_dir, ingest_shard_count: shard_count)

        {us, _} =
          :timer.tc(fn ->
            spans
            |> Enum.chunk_every(100)
            |> Enum.each(&TimelessTraces.Buffer.ingest/1)

            TimelessTraces.Buffer.flush()
            TimelessTraces.Index.sync()
          end)

        eps = span_count / (us / 1_000_000)
        {:ok, shard_stats} = TimelessTraces.Index.stats()

        {shard_count, us, eps, shard_stats.total_entries}
      end)

    Enum.each(shard_results, fn {shard_count, us, eps, total_entries} ->
      IO.puts(
        "  shards=#{String.pad_leading(Integer.to_string(shard_count), 2)}  " <>
          "#{fmt_ms(us)}  #{fmt_number(round(eps))} spans/sec  " <>
          "#{fmt_number(total_entries)} indexed"
      )
    end)

    IO.puts("")

    # Summary
    IO.puts("=== Summary ===")
    IO.puts("  Writer only:      #{fmt_number(round(writer_eps))} spans/sec")
    IO.puts("  Writer + Index:   #{fmt_number(round(idx_eps))} spans/sec")
    IO.puts("  Full pipeline:    #{fmt_number(round(pipe_eps))} spans/sec")

    Application.stop(:timeless_traces)
    File.rm_rf!(data_dir)
  end

  defp configure_app(data_dir, overrides \\ []) do
    File.mkdir_p!(Path.join(data_dir, "blocks"))
    Application.put_env(:timeless_traces, :data_dir, data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :compaction_interval, 600_000)

    Enum.each(overrides, fn {key, value} ->
      Application.put_env(:timeless_traces, key, value)
    end)
  end

  defp restart_traces(data_dir, overrides \\ []) do
    Application.stop(:timeless_traces)
    configure_app(data_dir, overrides)
    Application.ensure_all_started(:timeless_traces)
  end

  defp generate_spans(count) do
    base_ts = System.system_time(:nanosecond) - 86_400_000_000_000
    services = ~w(api-gateway user-service order-service payment-service inventory-service)
    names = ~w(HTTP\ GET HTTP\ POST db.query cache.get queue.publish grpc.call)
    kinds = [:server, :client, :internal, :producer, :consumer]
    methods = ~w(GET GET GET POST PUT DELETE)
    routes = ~w(/api/users /api/orders /api/products /api/checkout /health)

    Enum.map(1..count, fn i ->
      trace_id = random_hex(16)
      span_id = random_hex(8)
      ts = base_ts + i * 1_000_000
      duration = :rand.uniform(500_000_000)
      service = Enum.random(services)
      status = Enum.random([:ok, :ok, :ok, :ok, :error, :unset])

      %{
        trace_id: trace_id,
        span_id: span_id,
        parent_span_id: if(rem(i, 3) == 0, do: nil, else: random_hex(8)),
        name: Enum.random(names),
        kind: Enum.random(kinds),
        start_time: ts,
        end_time: ts + duration,
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
