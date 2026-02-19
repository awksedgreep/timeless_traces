defmodule TimelessTraces.Buffer do
  @moduledoc false

  use GenServer

  @max_in_flight System.schedulers_online()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec ingest([map()]) :: :ok
  def ingest(spans) when is_list(spans) do
    GenServer.cast(__MODULE__, {:ingest, spans})
  end

  @spec flush() :: :ok
  def flush do
    GenServer.call(__MODULE__, :flush, TimelessTraces.Config.query_timeout())
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    interval = TimelessTraces.Config.flush_interval()
    schedule_flush(interval)

    {:ok,
     %{
       buffer: [],
       buffer_size: 0,
       data_dir: data_dir,
       flush_interval: interval,
       in_flight: 0
     }}
  end

  @impl true
  def handle_cast({:ingest, spans}, state) do
    broadcast_to_subscribers(spans)
    buffer = spans ++ state.buffer
    size = state.buffer_size + length(spans)

    if size >= TimelessTraces.Config.max_buffer_size() do
      state = do_flush_async(buffer, state)
      {:noreply, %{state | buffer: [], buffer_size: 0}}
    else
      {:noreply, %{state | buffer: buffer, buffer_size: size}}
    end
  end

  @impl true
  def handle_call(:flush, _from, state) do
    do_flush(state.buffer, state.data_dir, sync: true)
    {:reply, :ok, %{state | buffer: [], buffer_size: 0}}
  end

  @impl true
  def handle_info(:flush_timer, state) do
    state =
      if state.buffer != [] do
        do_flush_async(state.buffer, state)
      else
        state
      end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: [], buffer_size: 0}}
  end

  def handle_info({:flush_done, _ref}, state) do
    {:noreply, %{state | in_flight: max(state.in_flight - 1, 0)}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | in_flight: max(state.in_flight - 1, 0)}}
  end

  defp do_flush_async(buffer, state) do
    if state.in_flight >= @max_in_flight do
      # Backpressure: fall back to sync flush
      do_flush(buffer, state.data_dir)
      state
    else
      entries = Enum.reverse(buffer)
      data_dir = state.data_dir
      caller = self()

      Task.Supervisor.start_child(TimelessTraces.FlushSupervisor, fn ->
        do_flush_work(entries, data_dir)
        send(caller, {:flush_done, make_ref()})
      end)

      %{state | in_flight: state.in_flight + 1}
    end
  end

  defp do_flush(buffer, data_dir, opts \\ [])
  defp do_flush([], _data_dir, _opts), do: :ok

  defp do_flush(buffer, data_dir, opts) do
    entries = Enum.reverse(buffer)
    do_flush_work(entries, data_dir, opts)
  end

  defp do_flush_work(entries, data_dir, opts \\ []) do
    start_time = System.monotonic_time()

    write_target = if TimelessTraces.Config.storage() == :memory, do: :memory, else: data_dir

    case TimelessTraces.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        {terms, trace_rows} = TimelessTraces.Index.precompute(entries)

        if Keyword.get(opts, :sync, false) do
          TimelessTraces.Index.index_block(block_meta, terms, trace_rows)
        else
          TimelessTraces.Index.index_block_async(block_meta, terms, trace_rows)
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
        IO.warn("TimelessTraces: failed to write block: #{inspect(reason)}")
    end
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_timer, interval)
  end

  defp broadcast_to_subscribers(spans) do
    case Registry.count_match(TimelessTraces.Registry, :spans, :_) do
      0 ->
        :ok

      _n ->
        span_structs =
          Enum.map(spans, fn span ->
            {span, TimelessTraces.Span.from_map(span)}
          end)

        Registry.dispatch(TimelessTraces.Registry, :spans, fn subscribers ->
          for {pid, opts} <- subscribers do
            for {span, span_struct} <- span_structs do
              if matches_subscription?(span, opts) do
                send(pid, {:timeless_traces, :span, span_struct})
              end
            end
          end
        end)
    end
  end

  defp matches_subscription?(_span, []), do: true

  defp matches_subscription?(span, opts) do
    Enum.all?(opts, fn
      {:name, name} ->
        span.name == name

      {:kind, kind} ->
        span.kind == kind

      {:status, status} ->
        span.status == status

      {:service, service} ->
        Map.get(span.attributes, "service.name") == service ||
          Map.get(span.resource || %{}, "service.name") == service

      _ ->
        true
    end)
  end
end
