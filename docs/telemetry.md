# Telemetry

TimelessTraces emits [telemetry](https://hex.pm/packages/telemetry) events for monitoring and observability.

## Events

### Flush

Emitted when the buffer flushes spans to a block.

**Event:** `[:timeless_traces, :flush, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Time taken to write the block |
| `entry_count` | integer | Number of spans in the block |
| `byte_size` | integer | Size of the written block in bytes |

| Metadata | Type | Description |
|----------|------|-------------|
| `block_id` | integer | ID of the written block |

### Query

Emitted when a query completes.

**Event:** `[:timeless_traces, :query, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Total query execution time |
| `total` | integer | Total matching spans (before pagination) |
| `blocks_read` | integer | Number of blocks decompressed |

| Metadata | Type | Description |
|----------|------|-------------|
| `filters` | keyword | The query filters used |

### Retention

Emitted when a retention run completes.

**Event:** `[:timeless_traces, :retention, :stop]`

| Measurement | Type | Description |
|-------------|------|-------------|
| `duration` | integer (native time) | Time taken for retention enforcement |
| `blocks_deleted` | integer | Number of blocks removed |

### Block error

Emitted when a block fails to read or decompress.

**Event:** `[:timeless_traces, :block, :error]`

| Metadata | Type | Description |
|----------|------|-------------|
| `file_path` | string | Path to the block file |
| `reason` | term | Error reason |

## Attaching handlers

### Basic logging

```elixir
:telemetry.attach_many(
  "timeless-traces-logger",
  [
    [:timeless_traces, :flush, :stop],
    [:timeless_traces, :query, :stop],
    [:timeless_traces, :retention, :stop],
    [:timeless_traces, :block, :error]
  ],
  fn event, measurements, metadata, _config ->
    require Logger
    Logger.info("#{inspect(event)}: #{inspect(measurements)} #{inspect(metadata)}")
  end,
  nil
)
```

### With telemetry_metrics

```elixir
defmodule MyApp.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(_arg) do
    children = [
      {Telemetry.Metrics.ConsoleReporter, metrics: metrics()}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp metrics do
    [
      summary("timeless_traces.flush.stop.duration",
        unit: {:native, :millisecond}),
      summary("timeless_traces.query.stop.duration",
        unit: {:native, :millisecond}),
      counter("timeless_traces.flush.stop.entry_count"),
      counter("timeless_traces.block.error")
    ]
  end
end
```

### With TimelessMetrics

If you're also running [TimelessMetrics](https://github.com/awksedgreep/timeless_metrics), you can write telemetry events as metrics:

```elixir
:telemetry.attach(
  "timeless-traces-to-metrics",
  [:timeless_traces, :flush, :stop],
  fn _event, %{entry_count: count, byte_size: bytes}, _metadata, _config ->
    TimelessMetrics.write(:metrics, "timeless_traces_flush_spans", %{}, count / 1)
    TimelessMetrics.write(:metrics, "timeless_traces_flush_bytes", %{}, bytes / 1)
  end,
  nil
)
```

### With TimelessLogs

If you're also running [TimelessLogs](https://github.com/awksedgreep/timeless_logs), telemetry events are automatically captured via `Logger` calls.
