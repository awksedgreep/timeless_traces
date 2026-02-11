#!/usr/bin/env elixir

# Compression sweet spot benchmark for SpanStream
# Tests various batch sizes to find optimal compression ratio

defmodule CompressionBench do
  def make_span(i, trace_id \\ nil) do
    now = System.system_time(:nanosecond)
    methods = ~w(GET POST PUT DELETE PATCH)
    routes = ~w(/api/users /api/orders /api/products /api/health /api/auth)
    kinds = [:server, :client, :internal]
    statuses = [:ok, :ok, :ok, :ok, :error]

    %{
      trace_id: trace_id || "trace-#{:rand.uniform(1000)}",
      span_id: "span-#{i}-#{:rand.uniform(100_000)}",
      parent_span_id: if(rem(i, 3) == 0, do: nil, else: "span-parent-#{i}"),
      name: "#{Enum.random(methods)} #{Enum.random(routes)}",
      kind: Enum.random(kinds),
      start_time: now + i * 1_000_000,
      end_time: now + i * 1_000_000 + :rand.uniform(500_000_000),
      duration_ns: :rand.uniform(500_000_000),
      status: Enum.random(statuses),
      status_message: if(:rand.uniform(10) == 1, do: "timeout after 5000ms", else: nil),
      attributes: %{
        "http.method" => Enum.random(methods),
        "http.route" => Enum.random(routes),
        "http.status_code" => Enum.random([200, 200, 200, 201, 400, 404, 500]),
        "service.name" => Enum.random(~w(api-gateway user-service order-service auth-service)),
        "net.peer.ip" => "10.0.#{:rand.uniform(255)}.#{:rand.uniform(255)}",
        "request_id" => "req-#{:crypto.strong_rand_bytes(8) |> Base.hex_encode32(case: :lower)}"
      },
      events: if(:rand.uniform(5) == 1, do: [%{name: "exception", timestamp: now}], else: []),
      resource: %{
        "service.name" => "my-app",
        "service.version" => "1.2.3",
        "host.name" => "prod-#{:rand.uniform(10)}"
      },
      instrumentation_scope: %{name: "otel_phoenix", version: "1.0.0"}
    }
  end

  def bench(count) do
    spans = for i <- 1..count, do: make_span(i)

    raw_binary = :erlang.term_to_binary(spans)
    raw_size = byte_size(raw_binary)

    compressed = :ezstd.compress(raw_binary)
    compressed_size = byte_size(compressed)

    ratio = Float.round(raw_size / compressed_size, 2)

    {count, raw_size, compressed_size, ratio}
  end

  def run do
    IO.puts("")
    IO.puts("SpanStream Compression Sweet Spot Benchmark")
    IO.puts(String.duplicate("=", 70))
    IO.puts("")

    header =
      String.pad_trailing("Spans", 8) <>
        String.pad_trailing("Raw Size", 14) <>
        String.pad_trailing("Compressed", 14) <>
        String.pad_trailing("Ratio", 10) <>
        "Per-Span Compressed"

    IO.puts(header)
    IO.puts(String.duplicate("-", 70))

    counts = [1, 2, 5, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000]

    for count <- counts do
      {count, raw, compressed, ratio} = bench(count)
      per_span = Float.round(compressed / count, 1)

      line =
        String.pad_trailing("#{count}", 8) <>
          String.pad_trailing(format_bytes(raw), 14) <>
          String.pad_trailing(format_bytes(compressed), 14) <>
          String.pad_trailing("#{ratio}x", 10) <>
          "#{per_span} bytes"

      IO.puts(line)
    end

    IO.puts("")
    IO.puts("Sweet spot: where ratio plateaus and per-span cost is minimized.")
    IO.puts("")
  end

  defp format_bytes(bytes) when bytes < 1024, do: "#{bytes} B"
  defp format_bytes(bytes) when bytes < 1_048_576, do: "#{Float.round(bytes / 1024, 1)} KB"
  defp format_bytes(bytes), do: "#{Float.round(bytes / 1_048_576, 1)} MB"
end

CompressionBench.run()
