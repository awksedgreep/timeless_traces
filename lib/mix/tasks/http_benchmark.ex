defmodule Mix.Tasks.TimelessTraces.HttpBenchmark do
  @moduledoc "Benchmark HTTP ingest and query throughput via Rocket"
  use Mix.Task

  @shortdoc "Benchmark HTTP endpoint throughput (ingest + query)"

  @port 10_499
  @warmup_requests 200
  @bench_duration_sec 5

  @impl true
  def run(_args) do
    data_dir = "http_bench_#{System.unique_integer([:positive])}"
    File.mkdir_p!(Path.join(data_dir, "blocks"))

    Application.put_env(:timeless_traces, :data_dir, data_dir)
    Application.put_env(:timeless_traces, :storage, :memory)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :http, false)

    Mix.Task.run("app.start")
    :inets.start()

    # Start HTTP server
    spec = TimelessTraces.HTTP.child_spec(port: @port)
    {:ok, _} = Supervisor.start_link([spec], strategy: :one_for_one)

    IO.puts("=== TimelessTraces HTTP Benchmark (Rocket) ===")
    IO.puts("  Port: #{@port}")
    IO.puts("  Duration per phase: #{@bench_duration_sec}s")
    IO.puts("")

    # --- Phase 1: Health endpoint (baseline, no processing) ---
    IO.puts("--- Phase 1: GET /health (baseline) ---")
    health_rps = bench_get("/health")
    IO.puts("  #{fmt_number(health_rps)} req/sec\n")

    # --- Phase 2: OTLP JSON ingest (single span per request) ---
    IO.puts("--- Phase 2: POST OTLP ingest (1 span/req) ---")
    single_body = otlp_body(1)
    ingest_1_rps = bench_post("/insert/opentelemetry/v1/traces", single_body, "application/json")
    IO.puts("  #{fmt_number(ingest_1_rps)} req/sec (#{fmt_number(ingest_1_rps)} spans/sec)\n")

    # --- Phase 3: OTLP JSON ingest (10 spans per request) ---
    IO.puts("--- Phase 3: POST OTLP ingest (10 spans/req) ---")
    batch_body = otlp_body(10)
    ingest_10_rps = bench_post("/insert/opentelemetry/v1/traces", batch_body, "application/json")

    IO.puts(
      "  #{fmt_number(ingest_10_rps)} req/sec (#{fmt_number(ingest_10_rps * 10)} spans/sec)\n"
    )

    # --- Phase 4: OTLP JSON ingest (100 spans per request) ---
    IO.puts("--- Phase 4: POST OTLP ingest (100 spans/req) ---")
    big_body = otlp_body(100)
    ingest_100_rps = bench_post("/insert/opentelemetry/v1/traces", big_body, "application/json")

    IO.puts(
      "  #{fmt_number(ingest_100_rps)} req/sec (#{fmt_number(ingest_100_rps * 100)} spans/sec)\n"
    )

    # --- Phase 5: Concurrent ingest (multiple connections) ---
    IO.puts("--- Phase 5: Concurrent ingest (8 workers, 10 spans/req) ---")

    conc_rps =
      bench_concurrent_post("/insert/opentelemetry/v1/traces", batch_body, "application/json", 8)

    IO.puts("  #{fmt_number(conc_rps)} req/sec (#{fmt_number(conc_rps * 10)} spans/sec)\n")

    # Reset state for query benchmarks with small dataset
    Application.stop(:timeless_traces)
    Application.ensure_all_started(:timeless_traces)
    seed_query_data(50)
    TimelessTraces.flush()

    # --- Phase 6: Jaeger services query ---
    IO.puts("--- Phase 6: GET /select/jaeger/api/services ---")
    svc_rps = bench_get("/select/jaeger/api/services")
    IO.puts("  #{fmt_number(svc_rps)} req/sec\n")

    # --- Phase 7: Jaeger trace search ---
    IO.puts("--- Phase 7: GET /select/jaeger/api/traces?service=bench-svc&limit=10 ---")
    search_rps = bench_get("/select/jaeger/api/traces?service=bench-svc&limit=10")
    IO.puts("  #{fmt_number(search_rps)} req/sec\n")

    # --- Phase 8: Jaeger get trace ---
    IO.puts("--- Phase 8: GET /select/jaeger/api/traces/:trace_id ---")
    trace_rps = bench_get("/select/jaeger/api/traces/benchtraceabcdef01")
    IO.puts("  #{fmt_number(trace_rps)} req/sec\n")

    # Summary
    IO.puts("=== Summary ===")
    IO.puts("  Health (baseline):      #{fmt_number(health_rps)} req/sec")
    IO.puts("  Ingest 1 span/req:      #{fmt_number(ingest_1_rps)} req/sec")

    IO.puts(
      "  Ingest 10 spans/req:    #{fmt_number(ingest_10_rps)} req/sec  (#{fmt_number(ingest_10_rps * 10)} spans/sec)"
    )

    IO.puts(
      "  Ingest 100 spans/req:   #{fmt_number(ingest_100_rps)} req/sec  (#{fmt_number(ingest_100_rps * 100)} spans/sec)"
    )

    IO.puts(
      "  Concurrent ingest (8x): #{fmt_number(conc_rps)} req/sec  (#{fmt_number(conc_rps * 10)} spans/sec)"
    )

    IO.puts("  Services query:         #{fmt_number(svc_rps)} req/sec")
    IO.puts("  Trace search:           #{fmt_number(search_rps)} req/sec")
    IO.puts("  Get trace:              #{fmt_number(trace_rps)} req/sec")

    Application.stop(:timeless_traces)
    File.rm_rf!(data_dir)
  end

  defp seed_query_data(count) do
    now = System.system_time(:nanosecond)

    spans =
      Enum.map(1..count, fn i ->
        %{
          trace_id: "benchtraceabcdef01",
          span_id: "span#{String.pad_leading("#{i}", 12, "0")}",
          parent_span_id: if(i == 1, do: nil, else: "span#{String.pad_leading("1", 12, "0")}"),
          name: "bench-op-#{i}",
          kind: :server,
          start_time: now - i * 1_000_000,
          end_time: now - i * 1_000_000 + 50_000_000,
          duration_ns: 50_000_000,
          status: :ok,
          status_message: nil,
          attributes: %{"service.name" => "bench-svc", "http.method" => "GET"},
          events: [],
          resource: %{"service.name" => "bench-svc"},
          instrumentation_scope: nil
        }
      end)

    TimelessTraces.Buffer.ingest(spans)
  end

  defp bench_get(path) do
    url = ~c"http://127.0.0.1:#{@port}#{path}"
    http_opts = [timeout: 30_000, connect_timeout: 5_000]
    req_opts = [body_format: :binary]

    # Warmup
    for _ <- 1..@warmup_requests do
      {:ok, _} = :httpc.request(:get, {url, []}, http_opts, req_opts)
    end

    # Timed run
    deadline = System.monotonic_time(:millisecond) + @bench_duration_sec * 1000

    count =
      run_until(deadline, 0, fn ->
        {:ok, _} = :httpc.request(:get, {url, []}, http_opts, req_opts)
      end)

    round(count / @bench_duration_sec)
  end

  defp bench_post(path, body, content_type) do
    url = ~c"http://127.0.0.1:#{@port}#{path}"
    ct = to_charlist(content_type)
    http_opts = [timeout: 30_000, connect_timeout: 5_000]
    req_opts = [body_format: :binary]

    # Warmup
    for _ <- 1..@warmup_requests do
      {:ok, _} = :httpc.request(:post, {url, [], ct, body}, http_opts, req_opts)
    end

    # Timed run
    deadline = System.monotonic_time(:millisecond) + @bench_duration_sec * 1000

    count =
      run_until(deadline, 0, fn ->
        {:ok, _} = :httpc.request(:post, {url, [], ct, body}, http_opts, req_opts)
      end)

    round(count / @bench_duration_sec)
  end

  defp bench_concurrent_post(path, body, content_type, workers) do
    url = ~c"http://127.0.0.1:#{@port}#{path}"
    ct = to_charlist(content_type)
    http_opts = [timeout: 30_000, connect_timeout: 5_000]
    req_opts = [body_format: :binary]

    # Warmup
    for _ <- 1..@warmup_requests do
      {:ok, _} = :httpc.request(:post, {url, [], ct, body}, http_opts, req_opts)
    end

    parent = self()
    deadline = System.monotonic_time(:millisecond) + @bench_duration_sec * 1000

    pids =
      for _ <- 1..workers do
        spawn_link(fn ->
          count =
            run_until(deadline, 0, fn ->
              {:ok, _} = :httpc.request(:post, {url, [], ct, body}, http_opts, req_opts)
            end)

          send(parent, {:done, count})
        end)
      end

    total =
      Enum.reduce(pids, 0, fn _pid, acc ->
        receive do
          {:done, count} -> acc + count
        end
      end)

    round(total / @bench_duration_sec)
  end

  defp run_until(deadline, count, fun) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      fun.()
      run_until(deadline, count + 1, fun)
    end
  end

  defp otlp_body(span_count) do
    spans =
      Enum.map(1..span_count, fn i ->
        ts = 1_700_000_000_000_000_000 + i * 1_000_000

        %{
          "traceId" => random_hex(16),
          "spanId" => random_hex(8),
          "parentSpanId" => "",
          "name" => "bench-op-#{i}",
          "kind" => 2,
          "startTimeUnixNano" => "#{ts}",
          "endTimeUnixNano" => "#{ts + 50_000_000}",
          "status" => %{"code" => 1},
          "attributes" => [
            %{"key" => "http.method", "value" => %{"stringValue" => "GET"}},
            %{"key" => "http.status_code", "value" => %{"intValue" => 200}}
          ],
          "events" => []
        }
      end)

    %{
      "resourceSpans" => [
        %{
          "resource" => %{
            "attributes" => [
              %{"key" => "service.name", "value" => %{"stringValue" => "bench-svc"}}
            ]
          },
          "scopeSpans" => [
            %{
              "scope" => %{"name" => "bench", "version" => "1.0"},
              "spans" => spans
            }
          ]
        }
      ]
    }
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp random_hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp fmt_number(n) when is_float(n), do: fmt_number(round(n))

  defp fmt_number(n) when n >= 1_000_000,
    do: "#{:erlang.float_to_binary(n / 1_000_000, decimals: 1)}M"

  defp fmt_number(n) when n >= 1_000, do: "#{:erlang.float_to_binary(n / 1_000, decimals: 1)}K"
  defp fmt_number(n), do: "#{n}"
end
