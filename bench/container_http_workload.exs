# Container HTTP workload for timeless_traces — auto-ramp OTLP span ingest
# plus Jaeger-compatible queries.
#
# Pure HTTP client (Finch). Run from a project that has Finch available:
#
#   cd ../timeless_metrics && mix run --no-start \
#     ../timeless_traces/bench/container_http_workload.exs \
#     --url http://127.0.0.1:39428 --writers 16 --batch 500
#
# Each POST is one OTLP/JSON body for one service containing ~batch/5
# traces of 3-7 spans. Ramps by halving the per-writer POST interval each
# step until saturation (write p99 > 100ms, error rate > 5%, or
# throughput < 60% of target). Query workers run a fixed Jaeger mix
# (service search, service+operation search, trace-by-id, services list)
# alongside writes the whole time.

defmodule TracesHttpWorkload do
  @p99_ceiling_us 100_000
  @error_rate_ceil 0.05
  @throughput_floor 0.60
  @min_interval_ms 2

  @services ~w(api web worker billing auth search notifications ingest)
  @operations %{
    "api" => ["GET /api/v1/users", "GET /api/v1/posts", "POST /api/v1/orders", "GET /health"],
    "web" => ["GET /dashboard", "GET /checkout", "POST /login"],
    "worker" => ["job.process_upload", "job.send_digest", "job.sync"],
    "billing" => ["POST /charge", "GET /invoices"],
    "auth" => ["POST /token", "GET /verify"],
    "search" => ["GET /search", "index.update"],
    "notifications" => ["push.send", "email.send"],
    "ingest" => ["POST /events", "batch.flush"]
  }
  @db_systems ~w(postgresql redis)
  @child_ops ["db.query", "cache.get", "http.call", "serialize", "auth.check"]

  def run do
    {:ok, _} = Application.ensure_all_started(:finch)
    Process.flag(:trap_exit, true)

    {opts, _, _} =
      OptionParser.parse(System.argv(),
        switches: [
          url: :string,
          writers: :integer,
          batch: :integer,
          step_seconds: :integer,
          start_interval: :float,
          query_workers: :integer,
          warmup: :integer
        ]
      )

    url = opts[:url] || "http://127.0.0.1:39428"
    writers = opts[:writers] || 16
    batch = opts[:batch] || 500
    step_dur = opts[:step_seconds] || 15
    start_interval = opts[:start_interval] || 1000.0
    query_workers = opts[:query_workers] || 10
    warmup_s = opts[:warmup] || 5

    IO.puts("")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  TimelessTraces Container HTTP Workload — Auto-Ramp")
    IO.puts("  " <> String.duplicate("=", 64))
    IO.puts("  Target:      #{url}")
    IO.puts("  Writers:     #{writers} x ~#{batch} spans/POST (OTLP JSON)")
    IO.puts("  Ramp:        interval ÷2 from #{trunc(start_interval)}ms until saturation")
    IO.puts("  Step dur:    #{step_dur}s (#{warmup_s}s warmup)")
    IO.puts("  Queries:     #{query_workers} workers: search_svc, search_op, trace_by_id, services")
    IO.puts("  " <> String.duplicate("=", 64))

    Finch.start_link(
      name: BenchFinch,
      pools: %{default: [size: writers + query_workers + 10, count: 1]}
    )

    verify(url)

    # Recently written trace ids, sampled by trace_by_id queries
    :ets.new(:recent_traces, [:named_table, :public, {:write_concurrency, true}])
    :atomics.new(1, []) |> then(&:persistent_term.put(:trace_ring_idx, &1))

    # Warmup
    Enum.each(1..warmup_s, fn _ ->
      Enum.each(1..writers, fn i -> post_spans(url, build_body(Enum.at(@services, rem(i, 8)), batch)) end)
      Process.sleep(1000)
    end)

    steps = ramp(url, writers, batch, step_dur, start_interval, query_workers, [])

    print_results(steps)
    print_health(url)
  end

  defp ramp(url, writers, batch, step_dur, interval_ms, query_workers, acc) do
    target_sps = writers * batch * 1000 / interval_ms

    IO.write(
      "  Step #{length(acc) + 1}: #{fmt_ms(interval_ms)} (~#{fmt_num(target_sps)} spans/s) ... "
    )

    write_ets = :ets.new(:w, [:duplicate_bag, :public, {:write_concurrency, true}])
    query_ets = :ets.new(:q, [:duplicate_bag, :public, {:write_concurrency, true}])
    ctr = :counters.new(4, [:atomics])
    stop = :atomics.new(1, [])

    writer_pids =
      Enum.map(1..writers, fn i ->
        service = Enum.at(@services, rem(i, length(@services)))

        spawn_link(fn ->
          Process.sleep(trunc(interval_ms * (i - 1) / writers))
          writer_loop(url, service, batch, interval_ms, write_ets, ctr, stop)
        end)
      end)

    query_pids =
      Enum.map(1..query_workers, fn i ->
        spawn_link(fn ->
          Process.sleep(i * 50)
          query_loop(url, query_ets, ctr, stop)
        end)
      end)

    Process.sleep(step_dur * 1000)
    :atomics.put(stop, 1, 1)
    Process.sleep(300)
    Enum.each(writer_pids ++ query_pids, &Process.exit(&1, :kill))

    w_lat = :ets.tab2list(write_ets) |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    q_lat = :ets.tab2list(query_ets) |> Enum.map(&elem(&1, 1)) |> Enum.sort()
    :ets.delete(write_ets)
    :ets.delete(query_ets)

    reqs = :counters.get(ctr, 1)
    werrs = :counters.get(ctr, 2)
    queries = :counters.get(ctr, 3)
    qerrs = :counters.get(ctr, 4)

    actual_sps = reqs * batch / step_dur
    err_rate = if reqs + werrs > 0, do: werrs / (reqs + werrs), else: 0.0
    p99 = pct(w_lat, 0.99)

    step = %{
      interval: interval_ms,
      target_sps: target_sps,
      actual_sps: actual_sps,
      reqs_s: reqs / step_dur,
      werrs: werrs,
      qps: queries / step_dur,
      qerrs: qerrs,
      w_p50: pct(w_lat, 0.50),
      w_p99: p99,
      w_p999: pct(w_lat, 0.999),
      q_p50: pct(q_lat, 0.50),
      q_p99: pct(q_lat, 0.99),
      q_p999: pct(q_lat, 0.999)
    }

    IO.puts("#{fmt_num(actual_sps)} spans/s, w_p99 #{fmt_us(p99)}, #{trunc(step.qps)} qps")

    saturated =
      p99 > @p99_ceiling_us or err_rate > @error_rate_ceil or
        actual_sps < target_sps * @throughput_floor or interval_ms / 2 < @min_interval_ms

    if saturated do
      reason =
        cond do
          p99 > @p99_ceiling_us -> "write p99 #{fmt_us(p99)} > 100ms"
          err_rate > @error_rate_ceil -> "error rate #{Float.round(err_rate * 100, 1)}%"
          actual_sps < target_sps * @throughput_floor -> "throughput #{fmt_num(actual_sps)} < 60% of target"
          true -> "min interval reached"
        end

      IO.puts("  >> Saturated: #{reason}")
      Enum.reverse([step | acc])
    else
      ramp(url, writers, batch, step_dur, interval_ms / 2, query_workers, [step | acc])
    end
  end

  defp writer_loop(url, service, batch, interval_ms, ets, ctr, stop) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      body = build_body(service, batch)
      t0 = System.monotonic_time(:microsecond)
      ok = post_spans(url, body)
      elapsed = System.monotonic_time(:microsecond) - t0

      if ok do
        :ets.insert(ets, {:l, elapsed})
        :counters.add(ctr, 1, 1)
      else
        :counters.add(ctr, 2, 1)
      end

      sleep = max(trunc(interval_ms) - div(elapsed, 1000), 0)
      if sleep > 0, do: Process.sleep(sleep)
      writer_loop(url, service, batch, interval_ms, ets, ctr, stop)
    end
  end

  defp query_loop(url, ets, ctr, stop) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      now_us = System.os_time(:microsecond)
      svc = Enum.random(@services)

      path =
        case :rand.uniform(10) do
          n when n <= 4 ->
            "/select/jaeger/api/traces?service=#{svc}&start=#{now_us - 300_000_000}&limit=20"

          n when n <= 6 ->
            op = URI.encode_www_form(Enum.random(@operations[svc]))
            "/select/jaeger/api/traces?service=#{svc}&operation=#{op}&start=#{now_us - 300_000_000}&limit=20"

          n when n <= 9 ->
            case sample_trace_id() do
              nil -> "/select/jaeger/api/services"
              tid -> "/select/jaeger/api/traces/#{tid}"
            end

          _ ->
            "/select/jaeger/api/services"
        end

      t0 = System.monotonic_time(:microsecond)
      req = Finch.build(:get, url <> path)

      case Finch.request(req, BenchFinch, receive_timeout: 30_000) do
        {:ok, %{status: s}} when s in 200..299 ->
          :ets.insert(ets, {:l, System.monotonic_time(:microsecond) - t0})
          :counters.add(ctr, 3, 1)

        _ ->
          :counters.add(ctr, 4, 1)
      end

      Process.sleep(50)
      query_loop(url, ets, ctr, stop)
    end
  end

  defp post_spans(url, body) do
    req =
      Finch.build(
        :post,
        url <> "/insert/opentelemetry/v1/traces",
        [{"content-type", "application/json"}],
        body
      )

    case Finch.request(req, BenchFinch, receive_timeout: 30_000) do
      {:ok, %{status: s}} when s in 200..299 -> true
      _ -> false
    end
  end

  # --- OTLP body generation: ~batch spans as traces of 3-7 spans ---

  defp build_body(service, span_budget) do
    now_ns = System.os_time(:nanosecond)
    traces = build_traces(service, span_budget, now_ns, [])

    ~s({"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"#{service}"}}]},"scopeSpans":[{"scope":{"name":"bench"},"spans":[) <>
      Enum.join(traces, ",") <> ~s(]}]}]})
  end

  defp build_traces(_service, budget, _now_ns, acc) when budget <= 0,
    do: acc |> Enum.reverse() |> List.flatten()

  defp build_traces(service, budget, now_ns, acc) do
    n_spans = min(2 + :rand.uniform(5), budget)
    trace_id = hex(16)
    remember_trace(trace_id)
    root_id = hex(8)
    root_op = Enum.random(@operations[service])
    root_dur = (1 + :rand.uniform(200)) * 1_000_000
    start_ns = now_ns - :rand.uniform(1_000_000_000)

    root =
      span_json(trace_id, root_id, "", root_op, 2, start_ns, start_ns + root_dur, [
        attr("http.method", "GET"),
        attr("http.status_code", "200")
      ])

    children =
      for _ <- 1..(n_spans - 1)//1 do
        child_start = start_ns + :rand.uniform(root_dur)
        child_dur = :rand.uniform(max(div(root_dur, 4), 1_000_000))

        span_json(trace_id, hex(8), root_id, Enum.random(@child_ops), 3, child_start,
          child_start + child_dur, [
            attr("db.system", Enum.random(@db_systems)),
            attr("db.statement", "SELECT * FROM t WHERE id = $1")
          ])
      end

    build_traces(service, budget - n_spans, now_ns, [[root | children] | acc])
  end

  defp span_json(tid, sid, parent, name, kind, start_ns, end_ns, attrs) do
    ~s({"traceId":"#{tid}","spanId":"#{sid}","parentSpanId":"#{parent}","name":"#{name}","kind":#{kind},"startTimeUnixNano":#{start_ns},"endTimeUnixNano":#{end_ns},"status":{"code":0},"attributes":[) <>
      Enum.join(attrs, ",") <> "]}"
  end

  defp attr(k, v), do: ~s({"key":"#{k}","value":{"stringValue":"#{v}"}})

  defp hex(bytes), do: Base.encode16(:crypto.strong_rand_bytes(bytes), case: :lower)

  # Bounded ring of recent trace ids for trace_by_id queries
  defp remember_trace(tid) do
    ctr = :persistent_term.get(:trace_ring_idx)
    idx = rem(:atomics.add_get(ctr, 1, 1), 4096)
    :ets.insert(:recent_traces, {idx, tid})
  end

  defp sample_trace_id do
    case :ets.lookup(:recent_traces, :rand.uniform(4096) - 1) do
      [{_, tid}] -> tid
      [] -> nil
    end
  end

  defp verify(url) do
    req = Finch.build(:get, url <> "/health")

    case Finch.request(req, BenchFinch, receive_timeout: 5_000) do
      {:ok, %{status: 200, body: body}} -> IO.puts("  Target OK: #{body}")
      other -> raise "target #{url} not healthy: #{inspect(other)}"
    end
  end

  defp print_health(url) do
    req = Finch.build(:get, url <> "/health")

    case Finch.request(req, BenchFinch, receive_timeout: 60_000) do
      {:ok, %{status: 200, body: body}} -> IO.puts("\n  Final /health: #{body}")
      other -> IO.puts("\n  Final /health failed: #{inspect(other)}")
    end
  end

  defp print_results(steps) do
    IO.puts("\n  Write Latency (spans)")
    IO.puts("  " <> String.duplicate("-", 78))

    IO.puts(
      "  " <>
        pad("Interval", 10) <>
        pad("Req/s", 8) <>
        pad("Spans/s", 12) <> pad("p50", 10) <> pad("p99", 10) <> pad("p999", 10) <> pad("errs", 6)
    )

    Enum.each(steps, fn s ->
      IO.puts(
        "  " <>
          pad(fmt_ms(s.interval), 10) <>
          pad("#{trunc(s.reqs_s)}", 8) <>
          pad(fmt_num(s.actual_sps), 12) <>
          pad(fmt_us(s.w_p50), 10) <>
          pad(fmt_us(s.w_p99), 10) <> pad(fmt_us(s.w_p999), 10) <> pad("#{s.werrs}", 6)
      )
    end)

    IO.puts("\n  Query Latency Under Write Load")
    IO.puts("  " <> String.duplicate("-", 78))

    IO.puts(
      "  " <>
        pad("W Spans/s", 12) <>
        pad("Q/s", 8) <> pad("p50", 10) <> pad("p99", 10) <> pad("p999", 10) <> pad("errs", 6)
    )

    Enum.each(steps, fn s ->
      IO.puts(
        "  " <>
          pad(fmt_num(s.actual_sps), 12) <>
          pad("#{Float.round(s.qps, 1)}", 8) <>
          pad(fmt_us(s.q_p50), 10) <>
          pad(fmt_us(s.q_p99), 10) <> pad(fmt_us(s.q_p999), 10) <> pad("#{s.qerrs}", 6)
      )
    end)

    peak = Enum.max_by(steps, & &1.actual_sps)
    IO.puts("\n  Peak ingest: #{fmt_num(peak.actual_sps)} spans/s (write p99 #{fmt_us(peak.w_p99)})")
  end

  defp pct([], _), do: 0

  defp pct(sorted, p) do
    idx = min(trunc(length(sorted) * p), length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  defp pad(s, n), do: String.pad_trailing(s, n)

  defp fmt_ms(ms) when ms >= 1000, do: "#{Float.round(ms / 1000, 1)}s"
  defp fmt_ms(ms), do: "#{trunc(ms)}ms"

  defp fmt_us(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)}s"
  defp fmt_us(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 2)}ms"
  defp fmt_us(us), do: "#{trunc(us)}us"

  defp fmt_num(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp fmt_num(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp fmt_num(n), do: "#{trunc(n)}"
end

TracesHttpWorkload.run()
