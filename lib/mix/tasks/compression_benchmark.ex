defmodule Mix.Tasks.TimelessTraces.CompressionBenchmark do
  @moduledoc "Benchmark zstd vs openzl columnar compression for span blocks"
  use Mix.Task

  @shortdoc "Compare zstd and openzl compression (size, speed, round-trip, query)"

  @impl true
  def run(_args) do
    IO.puts("=== TimelessTraces Compression Benchmark ===\n")

    span_count = 500_000
    block_size = 1000
    IO.puts("Pre-generating #{fmt_number(span_count)} spans...")
    spans = generate_spans(span_count)
    blocks = Enum.chunk_every(spans, block_size)
    block_count = length(blocks)
    IO.puts("#{block_count} blocks of #{block_size} spans each.\n")

    # Measure raw size for ratio calculation
    raw_bytes =
      Enum.reduce(blocks, 0, fn chunk, acc ->
        acc + byte_size(:erlang.term_to_binary(chunk))
      end)

    IO.puts("Raw (term_to_binary) total: #{fmt_bytes(raw_bytes)}\n")

    format_results =
      for format <- [:zstd, :openzl] do
        IO.puts("--- #{format} ---")

        # Compress all blocks
        {compress_us, compressed_blocks} =
          :timer.tc(fn ->
            Enum.map(blocks, fn chunk ->
              {:ok, meta} = TimelessTraces.Writer.write_block(chunk, :memory, format)
              {meta.data, meta.byte_size, chunk}
            end)
          end)

        total_bytes =
          Enum.reduce(compressed_blocks, 0, fn {_data, sz, _orig}, acc -> acc + sz end)

        ratio = raw_bytes / max(total_bytes, 1)

        IO.puts("  Compressed size: #{fmt_bytes(total_bytes)}")
        IO.puts("  Ratio vs raw:   #{:erlang.float_to_binary(ratio, decimals: 2)}x")
        IO.puts("  Compress time:   #{fmt_ms(compress_us)}")

        # Decompress all blocks and verify round-trip
        {decompress_us, errors} =
          :timer.tc(fn ->
            Enum.reduce(compressed_blocks, 0, fn {data, _sz, original}, err_count ->
              case TimelessTraces.Writer.decompress_block(data, format) do
                {:ok, recovered} ->
                  if length(recovered) != length(original) do
                    err_count + 1
                  else
                    if verify_roundtrip(original, recovered) do
                      err_count
                    else
                      err_count + 1
                    end
                  end

                {:error, _} ->
                  err_count + 1
              end
            end)
          end)

        IO.puts("  Decompress time: #{fmt_ms(decompress_us)}")

        if errors > 0 do
          IO.puts("  ERRORS: #{errors} blocks failed round-trip verification!")
        else
          IO.puts("  Round-trip: OK (all #{block_count} blocks verified)")
        end

        IO.puts("")

        {format, total_bytes, ratio, compress_us, decompress_us}
      end

    # Phase 2: End-to-end query benchmark
    IO.puts("=== Query Benchmark (disk, full pipeline) ===\n")

    queries = [
      {"all (limit 100)", [limit: 100]},
      {"status=error", [status: :error, limit: 100]},
      {"service=api-gateway", [service: "api-gateway", limit: 100]},
      {"kind=server", [kind: :server, limit: 100]}
    ]

    query_results =
      for format <- [:zstd, :openzl] do
        data_dir = "bench_query_#{format}_#{System.unique_integer([:positive])}"
        blocks_dir = Path.join(data_dir, "blocks")
        File.mkdir_p!(blocks_dir)

        Application.stop(:timeless_traces)
        Application.put_env(:timeless_traces, :data_dir, data_dir)
        Application.put_env(:timeless_traces, :storage, :disk)
        Application.put_env(:timeless_traces, :compaction_interval, 600_000)
        Application.ensure_all_started(:timeless_traces)

        # Write and index blocks in this format
        for chunk <- blocks do
          {:ok, meta} = TimelessTraces.Writer.write_block(chunk, data_dir, format)
          TimelessTraces.Index.index_block(meta, chunk)
        end

        IO.puts("--- #{format} queries (#{block_count} blocks on disk) ---")

        timings =
          for {label, filters} <- queries do
            # Warm-up run
            TimelessTraces.Index.query(filters)

            # 3 timed runs
            times =
              for _ <- 1..3 do
                {us, {:ok, result}} =
                  :timer.tc(fn -> TimelessTraces.Index.query(filters) end)

                {us, result.total}
              end

            avg_us = times |> Enum.map(&elem(&1, 0)) |> Enum.sum() |> div(3)
            {_, total} = hd(times)

            IO.puts(
              "  #{String.pad_trailing(label, 25)} #{String.pad_leading(fmt_ms(avg_us), 10)} avg  (#{fmt_number(total)} matches)"
            )

            {label, avg_us, total}
          end

        # Trace lookups
        sample_traces =
          spans
          |> Enum.map(& &1.trace_id)
          |> Enum.uniq()
          |> Enum.take_random(20)

        trace_times =
          for tid <- sample_traces do
            {us, {:ok, result}} = :timer.tc(fn -> TimelessTraces.Index.trace(tid) end)
            {us, length(result)}
          end

        avg_trace_us =
          trace_times
          |> Enum.map(&elem(&1, 0))
          |> Enum.sum()
          |> div(max(length(trace_times), 1))

        avg_spans =
          trace_times
          |> Enum.map(&elem(&1, 1))
          |> Enum.sum()
          |> div(max(length(trace_times), 1))

        IO.puts(
          "  #{String.pad_trailing("trace lookup (20x)", 25)} #{String.pad_leading(fmt_ms(avg_trace_us), 10)} avg  (#{avg_spans} spans/trace)"
        )

        IO.puts("")

        Application.stop(:timeless_traces)
        File.rm_rf!(data_dir)

        {format, timings, avg_trace_us}
      end

    # Final comparison table
    IO.puts("=== Summary ===\n")
    IO.puts("  Compression:")

    for {format, total_bytes, ratio, compress_us, decompress_us} <- format_results do
      IO.puts(
        "    #{format}: #{fmt_bytes(total_bytes)} (#{:erlang.float_to_binary(ratio, decimals: 1)}x)  compress #{fmt_ms(compress_us)}  decompress #{fmt_ms(decompress_us)}"
      )
    end

    IO.puts("\n  Query speed (avg over 3 runs):")

    [{_, zstd_timings, zstd_trace}, {_, ozl_timings, ozl_trace}] = query_results

    for {{label, zstd_us, _}, {_, ozl_us, _}} <- Enum.zip(zstd_timings, ozl_timings) do
      speedup = zstd_us / max(ozl_us, 1)

      IO.puts(
        "    #{String.pad_trailing(label, 25)} zstd: #{String.pad_leading(fmt_ms(zstd_us), 10)}  openzl: #{String.pad_leading(fmt_ms(ozl_us), 10)}  #{:erlang.float_to_binary(speedup, decimals: 1)}x"
      )
    end

    trace_speedup = zstd_trace / max(ozl_trace, 1)

    IO.puts(
      "    #{String.pad_trailing("trace lookup", 25)} zstd: #{String.pad_leading(fmt_ms(zstd_trace), 10)}  openzl: #{String.pad_leading(fmt_ms(ozl_trace), 10)}  #{:erlang.float_to_binary(trace_speedup, decimals: 1)}x"
    )
  end

  defp verify_roundtrip(original, recovered) do
    Enum.zip(original, recovered)
    |> Enum.all?(fn {orig, rec} ->
      orig.trace_id == rec.trace_id and
        orig.span_id == rec.span_id and
        orig.start_time == rec.start_time and
        orig.end_time == rec.end_time and
        orig.name == rec.name and
        orig.kind == rec.kind and
        orig.status == rec.status and
        orig.parent_span_id == rec.parent_span_id and
        orig.status_message == rec.status_message and
        orig.attributes == rec.attributes and
        orig.events == rec.events and
        orig.resource == rec.resource
    end)
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
        duration_ns: duration,
        status: status,
        status_message: if(status == :error, do: "internal error", else: nil),
        attributes: %{
          "service.name" => service,
          "http.method" => Enum.random(methods),
          "http.route" => Enum.random(routes),
          "http.status_code" => "#{Enum.random([200, 200, 200, 201, 400, 404, 500])}"
        },
        events: [],
        resource: %{"service.name" => service},
        instrumentation_scope: nil
      }
    end)
  end

  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"

  defp fmt_ms(us) when us < 1_000, do: "#{us}us"

  defp fmt_ms(us) when us < 1_000_000,
    do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"

  defp fmt_ms(us), do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"

  defp fmt_bytes(b) when b >= 1_048_576,
    do: "#{:erlang.float_to_binary(b / 1_048_576, decimals: 2)} MB"

  defp fmt_bytes(b) when b >= 1_024,
    do: "#{:erlang.float_to_binary(b / 1_024, decimals: 1)} KB"

  defp fmt_bytes(b), do: "#{b} B"
end
