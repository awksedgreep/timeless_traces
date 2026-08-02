# Deterministic, completion-aware HTTP workload for timeless_traces.
#
# Run from a project that has Finch available:
#
#   cd ../timeless_metrics
#   mix run --no-start ../timeless_traces/bench/container_http_workload.exs \
#     --url http://127.0.0.1:39428 --writers 16 --batch 500 \
#     --query-workers 2 --server-pid 12345
#
# A successful POST is admission, not durable completion. Every ramp step
# therefore stops producers, calls the service flush barrier, and computes
# completed spans/s over offered time plus drain time from the control
# counters. Query latency is retained per route shape, not only as one blend.

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
          warmup: :integer,
          seed: :integer,
          server_pid: :integer,
          max_steps: :integer
        ]
      )

    url = opts[:url] || "http://127.0.0.1:39428"
    writers = opts[:writers] || 16
    batch = opts[:batch] || 500
    step_dur = opts[:step_seconds] || 15
    start_interval = opts[:start_interval] || 1000.0
    query_workers = opts[:query_workers] || 2
    warmup_s = opts[:warmup] || 5
    seed = opts[:seed] || 20_260_802
    server_pid = opts[:server_pid]
    max_steps = opts[:max_steps] || 16
    base_ns = System.os_time(:nanosecond)

    IO.puts("")
    IO.puts("  " <> String.duplicate("=", 72))
    IO.puts("  TimelessTraces HTTP Workload — Durable Auto-Ramp")
    IO.puts("  " <> String.duplicate("=", 72))
    IO.puts("  Target:      #{url}")
    IO.puts("  Writers:     #{writers} x #{batch} spans/POST (OTLP JSON)")
    IO.puts("  Ramp:        interval ÷2 from #{trunc(start_interval)}ms")
    IO.puts("  Step dur:    #{step_dur}s offered + completion barrier")
    IO.puts("  Warmup:      #{warmup_s}s followed by a completion barrier")
    IO.puts("  Queries:     #{query_workers} deterministic workers")
    IO.puts("  Seed:        #{seed}")
    IO.puts("  Server PID:  #{server_pid || "not supplied"}")
    IO.puts("  " <> String.duplicate("=", 72))

    Finch.start_link(
      name: BenchFinch,
      pools: %{default: [size: writers + query_workers + 10, count: 1]}
    )

    verify(url)
    :ets.new(:recent_traces, [:named_table, :public, {:write_concurrency, true}])
    :atomics.new(1, []) |> then(&:persistent_term.put(:trace_ring_idx, &1))

    seed_process(seed, 0, 0)

    Enum.each(count_range(warmup_s), fn round ->
      Enum.each(count_range(writers), fn writer ->
        service = Enum.at(@services, rem(writer - 1, length(@services)))
        body = build_body(service, batch, {:warmup, writer, round}, base_ns)
        post_spans(url, body)
      end)

      Process.sleep(1_000)
    end)

    {_warmup_us, warmup_stats} = flush_barrier(url)
    assert_drained!(warmup_stats)

    steps =
      ramp(
        url,
        writers,
        batch,
        step_dur,
        start_interval,
        query_workers,
        seed,
        base_ns,
        server_pid,
        max_steps,
        []
      )

    print_results(steps)
    print_health(url)
  end

  defp ramp(
         _url,
         _writers,
         _batch,
         _step_dur,
         _interval_ms,
         _query_workers,
         _seed,
         _base_ns,
         _server_pid,
         0,
         acc
       ),
       do: Enum.reverse(acc)

  defp ramp(
         url,
         writers,
         batch,
         step_dur,
         interval_ms,
         query_workers,
         seed,
         base_ns,
         server_pid,
         steps_left,
         acc
       ) do
    step_index = length(acc) + 1
    target_sps = writers * batch * 1_000 / interval_ms
    before = data_plane(health(url))

    IO.write(
      "  Step #{step_index}: #{fmt_ms(interval_ms)} (~#{fmt_num(target_sps)} offered spans/s) ... "
    )

    write_ets = :ets.new(:w, [:duplicate_bag, :public, {:write_concurrency, true}])
    query_ets = :ets.new(:q, [:duplicate_bag, :public, {:write_concurrency, true}])
    ctr = :counters.new(4, [:atomics])
    stop = :atomics.new(1, [])

    writer_pids =
      Enum.map(count_range(writers), fn writer ->
        service = Enum.at(@services, rem(writer - 1, length(@services)))

        spawn_link(fn ->
          seed_process(seed, step_index, writer)
          Process.sleep(trunc(interval_ms * (writer - 1) / writers))

          writer_loop(
            url,
            service,
            writer,
            0,
            batch,
            interval_ms,
            base_ns,
            step_index,
            write_ets,
            ctr,
            stop
          )
        end)
      end)

    query_pids =
      Enum.map(count_range(query_workers), fn worker ->
        spawn_link(fn ->
          seed_process(seed, step_index + 10_000, worker)
          Process.sleep(worker * 50)
          query_loop(url, div(base_ns, 1_000), query_ets, ctr, stop)
        end)
      end)

    Process.sleep(step_dur * 1_000)
    :atomics.put(stop, 1, 1)
    Process.sleep(300)
    Enum.each(writer_pids ++ query_pids, &Process.exit(&1, :kill))

    {drain_us, barrier_stats} = flush_barrier(url)
    assert_drained!(barrier_stats)
    after_health = health(url)
    after_stats = data_plane(after_health)

    w_lat = :ets.tab2list(write_ets) |> Enum.map(&elem(&1, 1)) |> Enum.sort()

    q_samples =
      query_ets
      |> :ets.tab2list()
      |> Enum.map(fn {_key, shape, latency} -> {shape, latency} end)

    :ets.delete(write_ets)
    :ets.delete(query_ets)

    reqs = :counters.get(ctr, 1)
    werrs = :counters.get(ctr, 2)
    queries = :counters.get(ctr, 3)
    qerrs = :counters.get(ctr, 4)
    admitted = after_stats["admitted_spans"] - before["admitted_spans"]
    completed = after_stats["completed_spans"] - before["completed_spans"]
    failed = after_stats["failed_spans"] - before["failed_spans"]
    wall_seconds = step_dur + drain_us / 1_000_000
    admitted_sps = admitted / step_dur
    durable_sps = completed / wall_seconds
    err_rate = if reqs + werrs > 0, do: werrs / (reqs + werrs), else: 0.0
    write_p99 = pct(w_lat, 0.99)
    q_lat = q_samples |> Enum.map(&elem(&1, 1)) |> Enum.sort()

    step = %{
      interval: interval_ms,
      target_sps: target_sps,
      admitted_sps: admitted_sps,
      durable_sps: durable_sps,
      admitted: admitted,
      completed: completed,
      failed: failed,
      reqs_s: reqs / step_dur,
      werrs: werrs,
      drain_us: drain_us,
      qps: queries / step_dur,
      qerrs: qerrs,
      w_p50: pct(w_lat, 0.50),
      w_p95: pct(w_lat, 0.95),
      w_p99: write_p99,
      q_p50: pct(q_lat, 0.50),
      q_p95: pct(q_lat, 0.95),
      q_p99: pct(q_lat, 0.99),
      query_shapes: summarize_shapes(q_samples),
      blocks: after_health["blocks"],
      spans: after_health["spans"],
      disk_size: after_health["disk_size"],
      index_size: after_health["index_size"],
      memory: proc_memory(server_pid)
    }

    IO.puts(
      "#{fmt_num(durable_sps)} durable spans/s " <>
        "(#{fmt_num(admitted_sps)} admitted), drain #{fmt_us(drain_us)}, " <>
        "w_p99 #{fmt_us(write_p99)}, #{trunc(step.qps)} qps"
    )

    saturated =
      write_p99 > @p99_ceiling_us or err_rate > @error_rate_ceil or failed > 0 or
        durable_sps < target_sps * @throughput_floor or interval_ms / 2 < @min_interval_ms

    if saturated do
      reason =
        cond do
          write_p99 > @p99_ceiling_us -> "write p99 #{fmt_us(write_p99)} > 100ms"
          err_rate > @error_rate_ceil -> "HTTP error rate #{Float.round(err_rate * 100, 1)}%"
          failed > 0 -> "#{failed} storage failures"
          durable_sps < target_sps * @throughput_floor ->
            "durable throughput #{fmt_num(durable_sps)} < 60% of target"

          true ->
            "minimum interval reached"
        end

      IO.puts("  >> Saturated: #{reason}")
      Enum.reverse([step | acc])
    else
      ramp(
        url,
        writers,
        batch,
        step_dur,
        interval_ms / 2,
        query_workers,
        seed,
        base_ns,
        server_pid,
        steps_left - 1,
        [step | acc]
      )
    end
  end

  defp writer_loop(
         url,
         service,
         writer,
         sequence,
         batch,
         interval_ms,
         base_ns,
         step_index,
         ets,
         ctr,
         stop
       ) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      body = build_body(service, batch, {step_index, writer, sequence}, base_ns)
      t0 = System.monotonic_time(:microsecond)
      ok = post_spans(url, body)
      elapsed = System.monotonic_time(:microsecond) - t0

      if ok do
        :ets.insert(ets, {:latency, elapsed})
        :counters.add(ctr, 1, 1)
      else
        :counters.add(ctr, 2, 1)
      end

      sleep = max(trunc(interval_ms) - div(elapsed, 1_000), 0)
      if sleep > 0, do: Process.sleep(sleep)

      writer_loop(
        url,
        service,
        writer,
        sequence + 1,
        batch,
        interval_ms,
        base_ns,
        step_index,
        ets,
        ctr,
        stop
      )
    end
  end

  defp query_loop(url, base_us, ets, ctr, stop) do
    if :atomics.get(stop, 1) == 1 do
      :ok
    else
      svc = Enum.random(@services)

      {shape, path} =
        case :rand.uniform(10) do
          n when n <= 4 ->
            {:search_service,
             "/select/jaeger/api/traces?service=#{svc}&start=#{base_us - 300_000_000}&limit=20"}

          n when n <= 6 ->
            op = URI.encode_www_form(Enum.random(@operations[svc]))

            {:search_operation,
             "/select/jaeger/api/traces?service=#{svc}&operation=#{op}&start=#{base_us - 300_000_000}&limit=20"}

          n when n <= 9 ->
            case sample_trace_id() do
              nil -> {:services, "/select/jaeger/api/services"}
              tid -> {:trace_by_id, "/select/jaeger/api/traces/#{tid}"}
            end

          _ ->
            {:services, "/select/jaeger/api/services"}
        end

      t0 = System.monotonic_time(:microsecond)
      req = Finch.build(:get, url <> path)

      case Finch.request(req, BenchFinch, receive_timeout: 30_000) do
        {:ok, %{status: status}} when status in 200..299 ->
          :ets.insert(ets, {:latency, shape, System.monotonic_time(:microsecond) - t0})
          :counters.add(ctr, 3, 1)

        _ ->
          :counters.add(ctr, 4, 1)
      end

      Process.sleep(50)
      query_loop(url, base_us, ets, ctr, stop)
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
      {:ok, %{status: status}} when status in 200..299 -> true
      _ -> false
    end
  end

  # Every writer owns its PRNG stream and identity tuple. Scheduling can alter
  # row order, but not the generated request contents or query sequence.
  defp build_body(service, span_budget, identity, base_ns) do
    request_ns = base_ns + :erlang.phash2(identity, 1_000_000) * 1_000
    spans = build_traces(service, span_budget, request_ns, identity, 0, [])

    ~s({"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"#{service}"}}]},"scopeSpans":[{"scope":{"name":"bench"},"spans":[) <>
      Enum.join(spans, ",") <> ~s(]}]}]})
  end

  defp build_traces(_service, budget, _now_ns, _identity, _ordinal, acc) when budget <= 0,
    do: acc |> Enum.reverse() |> List.flatten()

  defp build_traces(service, budget, now_ns, identity, ordinal, acc) do
    span_count = min(2 + :rand.uniform(5), budget)
    trace_id = stable_hex(16, {:trace, identity, ordinal})
    remember_trace(trace_id)
    root_id = stable_hex(8, {:root, identity, ordinal})
    root_op = Enum.random(@operations[service])
    root_duration = (1 + :rand.uniform(200)) * 1_000_000
    start_ns = now_ns - :rand.uniform(1_000_000_000)

    root =
      span_json(trace_id, root_id, "", root_op, 2, start_ns, start_ns + root_duration, [
        attr("http.method", "GET"),
        attr("http.status_code", "200")
      ])

    children =
      if span_count > 1 do
        Enum.map(1..(span_count - 1), fn child ->
          child_start = start_ns + :rand.uniform(root_duration)
          child_duration = :rand.uniform(max(div(root_duration, 4), 1_000_000))

          span_json(
            trace_id,
            stable_hex(8, {:child, identity, ordinal, child}),
            root_id,
            Enum.random(@child_ops),
            3,
            child_start,
            child_start + child_duration,
            [
              attr("db.system", Enum.random(@db_systems)),
              attr("db.statement", "SELECT * FROM t WHERE id = $1")
            ]
          )
        end)
      else
        []
      end

    build_traces(
      service,
      budget - span_count,
      now_ns,
      identity,
      ordinal + 1,
      [[root | children] | acc]
    )
  end

  defp span_json(trace_id, span_id, parent, name, kind, start_ns, end_ns, attrs) do
    ~s({"traceId":"#{trace_id}","spanId":"#{span_id}","parentSpanId":"#{parent}","name":"#{name}","kind":#{kind},"startTimeUnixNano":#{start_ns},"endTimeUnixNano":#{end_ns},"status":{"code":0},"attributes":[) <>
      Enum.join(attrs, ",") <> "]}"
  end

  defp attr(key, value), do: ~s({"key":"#{key}","value":{"stringValue":"#{value}"}})

  defp stable_hex(bytes, identity) do
    identity
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> binary_part(0, bytes)
    |> Base.encode16(case: :lower)
  end

  defp remember_trace(trace_id) do
    counter = :persistent_term.get(:trace_ring_idx)
    index = rem(:atomics.add_get(counter, 1, 1), 4_096)
    :ets.insert(:recent_traces, {index, trace_id})
  end

  defp sample_trace_id do
    case :ets.lookup(:recent_traces, :rand.uniform(4_096) - 1) do
      [{_, trace_id}] -> trace_id
      [] -> nil
    end
  end

  defp verify(url) do
    body = health(url)

    unless is_map(body["data_plane"]) do
      raise "target lacks completion-aware data_plane health counters"
    end

    IO.puts("  Target OK: completion-aware health available")
  end

  defp health(url) do
    req = Finch.build(:get, url <> "/health")

    case Finch.request(req, BenchFinch, receive_timeout: 60_000) do
      {:ok, %{status: 200, body: body}} -> :json.decode(body)
      other -> raise "target #{url} not healthy: #{inspect(other)}"
    end
  end

  defp flush_barrier(url) do
    req = Finch.build(:get, url <> "/api/v1/flush")
    started = System.monotonic_time(:microsecond)

    case Finch.request(req, BenchFinch, receive_timeout: 120_000) do
      {:ok, %{status: 200, body: body}} ->
        {System.monotonic_time(:microsecond) - started, :json.decode(body)["data_plane"]}

      other ->
        raise "flush barrier failed: #{inspect(other)}"
    end
  end

  defp data_plane(health), do: health["data_plane"]

  defp assert_drained!(stats) do
    unless stats["queued_spans"] == 0 and stats["in_flight_spans"] == 0 and
             stats["in_flight_batches"] == 0 do
      raise "flush returned before drain: #{inspect(stats)}"
    end
  end

  defp summarize_shapes(samples) do
    samples
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Map.new(fn {shape, latencies} ->
      sorted = Enum.sort(latencies)

      {shape,
       %{
         count: length(sorted),
         p50: pct(sorted, 0.50),
         p95: pct(sorted, 0.95),
         p99: pct(sorted, 0.99)
       }}
    end)
  end

  defp proc_memory(nil), do: %{}

  defp proc_memory(pid) do
    case File.read("/proc/#{pid}/status") do
      {:ok, status} ->
        %{
          vm_hwm_kib: proc_kib(status, "VmHWM"),
          vm_rss_kib: proc_kib(status, "VmRSS")
        }

      _ ->
        %{}
    end
  end

  defp proc_kib(status, field) do
    case Regex.run(~r/^#{field}:\s+(\d+)\s+kB$/m, status) do
      [_, value] -> String.to_integer(value)
      _ -> nil
    end
  end

  defp print_health(url) do
    IO.puts("\n  Final /health: #{inspect(health(url), pretty: true)}")
  end

  defp print_results(steps) do
    IO.puts("\n  Write and Completion")
    IO.puts("  " <> String.duplicate("-", 104))

    IO.puts(
      "  " <>
        pad("Interval", 10) <>
        pad("Req/s", 8) <>
        pad("Admit/s", 12) <>
        pad("Durable/s", 12) <>
        pad("drain", 10) <>
        pad("w_p95", 10) <>
        pad("w_p99", 10) <>
        pad("failed", 8) <>
        pad("HWM KiB", 10)
    )

    Enum.each(steps, fn step ->
      IO.puts(
        "  " <>
          pad(fmt_ms(step.interval), 10) <>
          pad("#{trunc(step.reqs_s)}", 8) <>
          pad(fmt_num(step.admitted_sps), 12) <>
          pad(fmt_num(step.durable_sps), 12) <>
          pad(fmt_us(step.drain_us), 10) <>
          pad(fmt_us(step.w_p95), 10) <>
          pad(fmt_us(step.w_p99), 10) <>
          pad("#{step.failed}", 8) <>
          pad("#{step.memory[:vm_hwm_kib] || "-"}", 10)
      )
    end)

    IO.puts("\n  Query Latency Under Write Load")
    IO.puts("  " <> String.duplicate("-", 82))

    Enum.each(steps, fn step ->
      IO.puts(
        "  #{fmt_num(step.durable_sps)} durable spans/s: " <>
          "#{Float.round(step.qps, 1)} qps, p50 #{fmt_us(step.q_p50)}, " <>
          "p95 #{fmt_us(step.q_p95)}, p99 #{fmt_us(step.q_p99)}, errs #{step.qerrs}"
      )

      step.query_shapes
      |> Enum.sort()
      |> Enum.each(fn {shape, stats} ->
        IO.puts(
          "    #{pad(to_string(shape), 18)} n=#{pad(to_string(stats.count), 5)} " <>
            "p50=#{fmt_us(stats.p50)} p95=#{fmt_us(stats.p95)} p99=#{fmt_us(stats.p99)}"
        )
      end)
    end)

    peak = Enum.max_by(steps, & &1.durable_sps)
    final = List.last(steps)

    IO.puts(
      "\n  Peak durable ingest: #{fmt_num(peak.durable_sps)} spans/s " <>
        "(#{peak.completed} completed, #{peak.failed} failed, drain #{fmt_us(peak.drain_us)})"
    )

    IO.puts(
      "  Final storage: #{final.spans} spans, #{final.blocks} blocks, " <>
        "#{final.disk_size} block bytes, #{final.index_size} index bytes"
    )
  end

  defp seed_process(seed, group, member) do
    :rand.seed(:exsss, {
      rem(seed + 17 * group + member, 4_294_967_295) + 1,
      rem(seed * 3 + group + 31 * member, 4_294_967_295) + 1,
      rem(seed * 7 + 13 * group + 101 * member, 4_294_967_295) + 1
    })
  end

  defp count_range(count) when count > 0, do: 1..count
  defp count_range(_count), do: []

  defp pct([], _percentile), do: 0

  defp pct(sorted, percentile) do
    index = min(trunc(length(sorted) * percentile), length(sorted) - 1)
    Enum.at(sorted, index)
  end

  defp pad(value, width), do: value |> to_string() |> String.pad_trailing(width)
  defp fmt_ms(ms) when ms >= 1_000, do: "#{Float.round(ms / 1_000, 1)}s"
  defp fmt_ms(ms), do: "#{trunc(ms)}ms"
  defp fmt_us(us) when us >= 1_000_000, do: "#{Float.round(us / 1_000_000, 2)}s"
  defp fmt_us(us) when us >= 1_000, do: "#{Float.round(us / 1_000, 2)}ms"
  defp fmt_us(us), do: "#{trunc(us)}us"
  defp fmt_num(number) when number >= 1_000_000, do: "#{Float.round(number / 1_000_000, 1)}M"
  defp fmt_num(number) when number >= 1_000, do: "#{Float.round(number / 1_000, 1)}K"
  defp fmt_num(number), do: "#{trunc(number)}"
end

TracesHttpWorkload.run()
