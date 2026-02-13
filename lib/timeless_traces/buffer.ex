defmodule TimelessTraces.Buffer do
  @moduledoc false

  use GenServer

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

    {:ok, %{buffer: [], buffer_size: 0, data_dir: data_dir, flush_interval: interval}}
  end

  @impl true
  def handle_cast({:ingest, spans}, state) do
    broadcast_to_subscribers(spans)
    buffer = Enum.reverse(spans) ++ state.buffer
    size = state.buffer_size + length(spans)

    if size >= TimelessTraces.Config.max_buffer_size() do
      do_flush(buffer, state.data_dir)
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
    if state.buffer != [] do
      do_flush(state.buffer, state.data_dir)
    end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: [], buffer_size: 0}}
  end

  defp do_flush(buffer, data_dir, opts \\ [])
  defp do_flush([], _data_dir, _opts), do: :ok

  defp do_flush(buffer, data_dir, opts) do
    entries = Enum.reverse(buffer)
    start_time = System.monotonic_time()

    write_target = if TimelessTraces.Config.storage() == :memory, do: :memory, else: data_dir

    case TimelessTraces.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        if Keyword.get(opts, :sync, false) do
          TimelessTraces.Index.index_block(block_meta, entries)
        else
          TimelessTraces.Index.index_block_async(block_meta, entries)
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
        for span <- spans do
          span_struct = TimelessTraces.Span.from_map(span)

          Registry.dispatch(TimelessTraces.Registry, :spans, fn subscribers ->
            for {pid, opts} <- subscribers do
              if matches_subscription?(span, opts) do
                send(pid, {:timeless_traces, :span, span_struct})
              end
            end
          end)
        end
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
