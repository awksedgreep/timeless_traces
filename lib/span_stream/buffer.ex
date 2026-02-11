defmodule SpanStream.Buffer do
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
    GenServer.call(__MODULE__, :flush, SpanStream.Config.query_timeout())
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    interval = SpanStream.Config.flush_interval()
    schedule_flush(interval)

    {:ok, %{buffer: [], data_dir: data_dir, flush_interval: interval}}
  end

  @impl true
  def handle_cast({:ingest, spans}, state) do
    for span <- spans, do: broadcast_to_subscribers(span)
    buffer = Enum.reverse(spans) ++ state.buffer

    if length(buffer) >= SpanStream.Config.max_buffer_size() do
      do_flush(buffer, state.data_dir)
      {:noreply, %{state | buffer: []}}
    else
      {:noreply, %{state | buffer: buffer}}
    end
  end

  @impl true
  def handle_call(:flush, _from, state) do
    do_flush(state.buffer, state.data_dir)
    {:reply, :ok, %{state | buffer: []}}
  end

  @impl true
  def handle_info(:flush_timer, state) do
    if state.buffer != [] do
      do_flush(state.buffer, state.data_dir)
    end

    schedule_flush(state.flush_interval)
    {:noreply, %{state | buffer: []}}
  end

  defp do_flush([], _data_dir), do: :ok

  defp do_flush(buffer, data_dir) do
    entries = Enum.reverse(buffer)
    start_time = System.monotonic_time()

    write_target = if SpanStream.Config.storage() == :memory, do: :memory, else: data_dir

    case SpanStream.Writer.write_block(entries, write_target, :raw) do
      {:ok, block_meta} ->
        SpanStream.Index.index_block(block_meta, entries)
        duration = System.monotonic_time() - start_time

        SpanStream.Telemetry.event(
          [:span_stream, :flush, :stop],
          %{
            duration: duration,
            entry_count: block_meta.entry_count,
            byte_size: block_meta.byte_size
          },
          %{block_id: block_meta.block_id}
        )

      {:error, reason} ->
        IO.warn("SpanStream: failed to write block: #{inspect(reason)}")
    end
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_timer, interval)
  end

  defp broadcast_to_subscribers(span) do
    span_struct = SpanStream.Span.from_map(span)

    Registry.dispatch(SpanStream.Registry, :spans, fn subscribers ->
      for {pid, opts} <- subscribers do
        if matches_subscription?(span, opts) do
          send(pid, {:span_stream, :span, span_struct})
        end
      end
    end)
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
