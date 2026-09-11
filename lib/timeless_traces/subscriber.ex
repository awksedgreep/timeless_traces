defmodule TimelessTraces.Subscriber do
  @moduledoc false

  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec broadcast([map()]) :: :ok
  def broadcast([]), do: :ok

  def broadcast(spans) do
    GenServer.cast(__MODULE__, {:broadcast, spans})
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_cast({:broadcast, spans}, state) do
    if Registry.count_match(TimelessTraces.Registry, :spans, :_) > 0 do
      span_structs = Enum.map(spans, &{&1, TimelessTraces.Span.from_map(&1)})

      Registry.dispatch(TimelessTraces.Registry, :spans, fn subscribers ->
        Enum.each(subscribers, fn {pid, filters} ->
          prepared = TimelessTraces.Filter.prepare(filters)

          Enum.each(span_structs, fn {span, span_struct} ->
            if TimelessTraces.Filter.matches_prepared?(span, prepared) do
              send(pid, {:timeless_traces, :span, span_struct})
            end
          end)
        end)
      end)
    end

    {:noreply, state}
  end
end
