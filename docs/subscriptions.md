# Live Tail (Subscriptions)

TimelessTraces supports real-time span subscriptions. Subscriber processes receive spans as they arrive, before they're written to storage.

## Subscribing

```elixir
TimelessTraces.subscribe()
```

The calling process will receive messages of the form:

```elixir
{:timeless_traces, :span, %TimelessTraces.Span{}}
```

### Filtered subscriptions

Filter by span name, kind, status, or service:

```elixir
# Only receive error spans
TimelessTraces.subscribe(status: :error)

# Only receive server spans
TimelessTraces.subscribe(kind: :server)

# Only receive spans from a specific service
TimelessTraces.subscribe(service: "api-gateway")

# Combined filters
TimelessTraces.subscribe(status: :error, service: "api-gateway")
```

## Receiving spans

```elixir
TimelessTraces.subscribe(status: :error)

receive do
  {:timeless_traces, :span, %TimelessTraces.Span{} = span} ->
    IO.puts("[#{span.status}] #{span.name} (#{span.duration_ns / 1_000_000}ms)")
end
```

### In a GenServer

```elixir
defmodule MyApp.TraceWatcher do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    TimelessTraces.subscribe(status: :error)
    {:ok, %{}}
  end

  @impl true
  def handle_info({:timeless_traces, :span, span}, state) do
    # React to error spans: send alert, increment counter, etc.
    IO.puts("ERROR: #{span.name} from #{Map.get(span.attributes, "service.name")}")
    {:noreply, state}
  end
end
```

### In a LiveView

```elixir
defmodule MyAppWeb.TraceLive do
  use Phoenix.LiveView

  def mount(_params, _session, socket) do
    if connected?(socket) do
      TimelessTraces.subscribe(status: :error)
    end

    {:ok, assign(socket, spans: [])}
  end

  def handle_info({:timeless_traces, :span, span}, socket) do
    spans = [span | socket.assigns.spans] |> Enum.take(100)
    {:noreply, assign(socket, spans: spans)}
  end
end
```

## Unsubscribing

```elixir
TimelessTraces.unsubscribe()
```

Subscriptions are automatically cleaned up when the subscriber process exits.

## How it works

Subscriptions use an Elixir `Registry` with `:duplicate` keys. When the Buffer receives new spans, it broadcasts to all registered subscribers before flushing. This means subscribers see spans immediately, even before they're written to storage.

Filter matching happens at broadcast time -- subscribers only receive spans that match their filter criteria.

## Performance considerations

- Subscription delivery is synchronous with the Buffer's ingest path. A slow subscriber can affect ingest throughput.
- For high-volume span streams, filter at the subscription level (`:status`, `:kind`, `:service`, `:name`) rather than in the subscriber's `handle_info`.
- If you need to do expensive work in response to spans, send yourself a message and process it asynchronously.
