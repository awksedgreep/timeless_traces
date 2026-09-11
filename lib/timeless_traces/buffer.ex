defmodule TimelessTraces.Buffer do
  @moduledoc false

  use GenServer

  require Logger

  @max_in_flight System.schedulers_online()

  @type buffer_state :: %{
          buffer: [map()],
          buffer_size: non_neg_integer(),
          data_dir: String.t(),
          flush_interval: pos_integer(),
          in_flight: non_neg_integer(),
          pending_batches: :queue.queue([map()]),
          flush_waiters: [GenServer.from()]
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec ingest([map()]) :: :ok
  def ingest(spans) when is_list(spans) do
    # Producer-side tail insert makes spans queryable the moment this
    # function returns, independent of shard mailbox latency.
    TimelessTraces.HotTail.insert_many(spans)
    TimelessTraces.DataPlaneStats.admit_spans(length(spans))

    shard_count = TimelessTraces.BufferShard.count()

    spans
    |> Enum.group_by(&TimelessTraces.BufferShard.shard_for(&1, shard_count))
    |> Enum.each(fn {shard, shard_spans} ->
      if TimelessTraces.IngestPressure.overloaded?(shard) do
        # Above the watermark the producer blocks here until the drain
        # (write + index) frees capacity. Nothing is dropped or refused.
        TimelessTraces.Telemetry.event(
          [:timeless_traces, :ingest, :backpressure],
          %{span_count: length(shard_spans)},
          %{shard: shard}
        )

        wait_for_capacity(shard, TimelessTraces.Config.ingest_backpressure_timeout())
      end

      TimelessTraces.IngestPressure.add(shard, length(shard_spans))
      GenServer.cast(TimelessTraces.BufferShard.name(shard), {:ingest, shard_spans})
    end)

    TimelessTraces.Subscriber.broadcast(spans)
    :ok
  end

  defp wait_for_capacity(shard, timeout) do
    case TimelessTraces.IngestPressure.await_capacity(shard, timeout) do
      :ok ->
        :ok

      :timeout ->
        # Drain has stalled outright (e.g. dead disk). Accept anyway — losing
        # spans during normal operation is not acceptable — but say so loudly.
        Logger.error(
          "TimelessTraces: ingest backpressure wait timed out on shard #{shard}; accepting anyway"
        )

        :ok
    end
  end

  @spec flush() :: :ok
  def flush do
    target = TimelessTraces.DataPlaneStats.snapshot().admitted_spans
    deadline = System.monotonic_time(:millisecond) + TimelessTraces.Config.query_timeout()
    drain_to(target, deadline)
  end

  defp drain_to(target, deadline) do
    timeout = TimelessTraces.Config.query_timeout()

    0..(TimelessTraces.BufferShard.count() - 1)
    |> Task.async_stream(
      fn shard -> GenServer.call(TimelessTraces.BufferShard.name(shard), :flush, timeout) end,
      ordered: false,
      max_concurrency: TimelessTraces.BufferShard.count(),
      timeout: timeout
    )
    |> Enum.each(fn
      {:ok, :ok} -> :ok
      {:ok, other} -> raise "TimelessTraces shard flush failed: #{inspect(other)}"
      {:exit, reason} -> exit(reason)
    end)

    # Buffer tasks publish block metadata asynchronously. A flush is not a
    # durability barrier until that index mailbox is committed as well.
    TimelessTraces.Index.sync()

    stats = TimelessTraces.DataPlaneStats.snapshot()

    if stats.queued_spans == 0 and stats.in_flight_spans == 0 and
         stats.completed_spans + stats.failed_spans >= target do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "TimelessTraces flush timed out before durable drain: #{inspect(stats)}"
      end

      # Calls from this process can overtake casts sent by independent HTTP
      # processes. Repeat after a short yield until the producer-side gauge
      # and durable completion counter agree.
      Process.sleep(5)
      drain_to(target, deadline)
    end
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    shard = Keyword.fetch!(opts, :shard)
    interval = TimelessTraces.Config.flush_interval()
    schedule_flush(interval)

    # A restart drops whatever was in the mailbox/state; the producer-side
    # gauge must not carry those phantom spans forward.
    TimelessTraces.IngestPressure.reset(shard)

    {:ok,
     %{
       buffer: [],
       buffer_size: 0,
       data_dir: data_dir,
       shard: shard,
       flush_interval: interval,
       in_flight: 0,
       pending_batches: :queue.new(),
       flush_waiters: []
     }}
  end

  @impl true
  def handle_cast({:ingest, spans}, state) do
    buffer = spans ++ state.buffer
    size = state.buffer_size + length(spans)

    if size >= TimelessTraces.Config.max_buffer_size() do
      state = dispatch_or_queue_batch(buffer, state)
      {:noreply, %{state | buffer: [], buffer_size: 0}}
    else
      {:noreply, %{state | buffer: buffer, buffer_size: size}}
    end
  end

  @impl true
  def handle_call(:flush, from, state) do
    state =
      if state.buffer != [] do
        do_flush(state.buffer, state.data_dir, sync: true)
        TimelessTraces.IngestPressure.sub(state.shard, state.buffer_size)
        %{state | buffer: [], buffer_size: 0}
      else
        state
      end

    state = dispatch_queued_batches(state)

    if idle?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | flush_waiters: [from | state.flush_waiters]}}
    end
  end

  @impl true
  def handle_info(:flush_timer, state) do
    state =
      if state.buffer != [] do
        dispatch_or_queue_batch(state.buffer, state)
      else
        state
      end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: [], buffer_size: 0}}
  end

  def handle_info({:flush_done, _ref}, state) do
    state = %{state | in_flight: max(state.in_flight - 1, 0)}
    {:noreply, state |> dispatch_queued_batches() |> maybe_reply_flush_waiters()}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    state = %{state | in_flight: max(state.in_flight - 1, 0)}
    {:noreply, state |> dispatch_queued_batches() |> maybe_reply_flush_waiters()}
  end

  defp dispatch_or_queue_batch([], state), do: state

  defp dispatch_or_queue_batch(buffer, state) do
    entries = Enum.reverse(buffer)

    if state.in_flight < @max_in_flight do
      start_flush_task(state, entries)
    else
      %{state | pending_batches: :queue.in(entries, state.pending_batches)}
    end
  end

  defp dispatch_queued_batches(state) do
    if state.in_flight >= @max_in_flight do
      state
    else
      case :queue.out(state.pending_batches) do
        {{:value, entries}, rest} ->
          state
          |> Map.put(:pending_batches, rest)
          |> start_flush_task(entries)
          |> dispatch_queued_batches()

        {:empty, _queue} ->
          state
      end
    end
  end

  defp start_flush_task(state, entries) do
    data_dir = state.data_dir
    shard = state.shard
    caller = self()
    entry_count = length(entries)
    TimelessTraces.DataPlaneStats.flush_started(entry_count)

    Task.Supervisor.start_child(TimelessTraces.FlushSupervisor, fn ->
      try do
        do_flush_work(entries, data_dir, shard: shard)
      after
        TimelessTraces.DataPlaneStats.flush_finished(entry_count)
        send(caller, {:flush_done, make_ref()})
      end
    end)

    %{state | in_flight: state.in_flight + 1}
  end

  defp maybe_reply_flush_waiters(state) do
    if idle?(state) and state.flush_waiters != [] do
      Enum.each(state.flush_waiters, &GenServer.reply(&1, :ok))
      %{state | flush_waiters: []}
    else
      state
    end
  end

  defp idle?(state) do
    state.buffer == [] and state.in_flight == 0 and :queue.is_empty(state.pending_batches)
  end

  defp do_flush(buffer, data_dir, opts) do
    entries = Enum.reverse(buffer)
    do_flush_work(entries, data_dir, opts)
  end

  defp do_flush_work(entries, data_dir, opts) do
    start_time = System.monotonic_time()

    write_target = if TimelessTraces.Config.storage() == :memory, do: :memory, else: data_dir

    case TimelessTraces.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        {terms, trace_rows} = TimelessTraces.Index.precompute(entries)

        if Keyword.get(opts, :sync, false) do
          TimelessTraces.Index.index_block(block_meta, terms, trace_rows)
        else
          TimelessTraces.Index.index_block_async(
            block_meta,
            terms,
            trace_rows,
            Keyword.get(opts, :shard)
          )
        end

        duration = System.monotonic_time() - start_time

        TimelessTraces.Telemetry.event(
          [:timeless_traces, :flush, :stop],
          %{
            duration: duration,
            entry_count: block_meta.entry_count,
            byte_size: block_meta.byte_size
          },
          %{block_id: block_meta.block_id}
        )

      {:error, reason} ->
        Logger.error("TimelessTraces: failed to write block: #{inspect(reason)}")
        credit_gauge_on_drop(opts, entries)
        TimelessTraces.DataPlaneStats.fail_spans(length(entries))

        TimelessTraces.Telemetry.event(
          [:timeless_traces, :flush, :error],
          %{entry_count: length(entries)},
          %{reason: reason}
        )
    end
  rescue
    e ->
      Logger.error(
        "TimelessTraces: flush crashed: #{Exception.format(:error, e, __STACKTRACE__)}"
      )

      credit_gauge_on_drop(opts, entries)
      TimelessTraces.DataPlaneStats.fail_spans(length(entries))

      TimelessTraces.Telemetry.event(
        [:timeless_traces, :flush, :error],
        %{entry_count: length(entries)},
        %{reason: e}
      )
  end

  # Dropped spans must still release their gauge reservation or the
  # watermark ratchets shut over time.
  defp credit_gauge_on_drop(opts, entries) do
    case Keyword.get(opts, :shard) do
      nil -> :ok
      shard -> TimelessTraces.IngestPressure.sub(shard, length(entries))
    end
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_timer, interval)
  end
end
