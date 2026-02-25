defmodule TimelessTraces do
  @moduledoc """
  Embedded OpenTelemetry span storage and compression for Elixir applications.

  TimelessTraces receives spans from the OpenTelemetry Erlang SDK, compresses them
  into zstd blocks, and indexes them in SQLite for fast querying.

  ## Setup

      # config/config.exs
      config :timeless_traces,
        data_dir: "priv/span_stream"

      # Enable the OTel exporter
      config :opentelemetry,
        traces_exporter: {TimelessTraces.Exporter, []}

  ## Querying

      # Find error spans
      TimelessTraces.query(status: :error)

      # Find spans by service and kind
      TimelessTraces.query(service: "my_app", kind: :server)

      # Find slow spans (> 100ms)
      TimelessTraces.query(min_duration: 100_000_000)

  ## Trace Lookup

      # Get all spans in a trace
      TimelessTraces.trace("abc123def456...")
  """

  @doc """
  Query stored spans. Returns a `TimelessTraces.Result` struct.

  ## Filters

    * `:name` - Substring match on span name
    * `:service` - Match `service.name` attribute or resource
    * `:kind` - Span kind atom (`:internal`, `:server`, `:client`, `:producer`, `:consumer`)
    * `:status` - Status atom (`:ok`, `:error`, `:unset`)
    * `:min_duration` - Minimum duration in nanoseconds
    * `:max_duration` - Maximum duration in nanoseconds
    * `:since` - Start time lower bound (DateTime or unix nanos)
    * `:until` - Start time upper bound (DateTime or unix nanos)
    * `:trace_id` - Filter to specific trace ID
    * `:attributes` - Map of attribute key/value pairs to match

  ## Pagination & Ordering

    * `:limit` - Max entries to return (default 100)
    * `:offset` - Number of entries to skip (default 0)
    * `:order` - `:desc` (newest first, default) or `:asc` (oldest first)
  """
  @spec query(keyword()) :: {:ok, TimelessTraces.Result.t()} | {:error, term()}
  def query(filters \\ []) do
    TimelessTraces.Index.query(filters)
  end

  @doc """
  Retrieve all spans for a given trace ID, sorted by start time.

  ## Examples

      {:ok, spans} = TimelessTraces.trace("abc123...")
  """
  @spec trace(String.t()) :: {:ok, [TimelessTraces.Span.t()]}
  def trace(trace_id) do
    TimelessTraces.Index.trace(trace_id)
  end

  @doc """
  List all distinct service names found in stored spans.

  ## Examples

      {:ok, services} = TimelessTraces.services()
      #=> {:ok, ["my_app", "api_gateway", "auth_service"]}
  """
  @spec services() :: {:ok, [String.t()]}
  def services do
    TimelessTraces.Index.distinct_services()
  end

  @doc """
  List all distinct operation (span) names for a given service.

  ## Examples

      {:ok, ops} = TimelessTraces.operations("my_app")
      #=> {:ok, ["GET /users", "DB query", "cache_lookup"]}
  """
  @spec operations(String.t()) :: {:ok, [String.t()]}
  def operations(service) do
    TimelessTraces.Index.distinct_operations(service)
  end

  @doc """
  Flush the buffer, writing any pending spans to storage immediately.
  """
  @spec flush() :: :ok
  def flush do
    TimelessTraces.Buffer.flush()
  end

  @doc """
  Return aggregate statistics about stored span data.

  ## Examples

      {:ok, stats} = TimelessTraces.stats()
      stats.total_blocks   #=> 42
      stats.total_entries   #=> 50000
  """
  @spec stats() :: {:ok, TimelessTraces.Stats.t()}
  def stats do
    TimelessTraces.Index.stats()
  end

  @doc """
  Subscribe the calling process to receive new spans as they arrive.

  The subscriber receives messages of the form:
  `{:timeless_traces, :span, %TimelessTraces.Span{}}`.

  ## Options

    * `:name` - Only receive spans with this name
    * `:kind` - Only receive spans of this kind
    * `:status` - Only receive spans with this status
    * `:service` - Only receive spans from this service
  """
  @spec subscribe(keyword()) :: {:ok, pid()}
  def subscribe(opts \\ []) do
    Registry.register(TimelessTraces.Registry, :spans, opts)
  end

  @doc """
  Unsubscribe the calling process from span notifications.
  """
  @spec unsubscribe() :: :ok
  def unsubscribe do
    Registry.unregister(TimelessTraces.Registry, :spans)
  end

  @doc """
  Merge multiple small compressed blocks into fewer, larger blocks.

  Returns `:ok` if blocks were merged, `:noop` if no merge was needed.
  """
  @spec merge_now() :: :ok | :noop
  defdelegate merge_now(), to: TimelessTraces.Compactor

  @doc """
  Create a consistent online backup of the span store.

  Flushes all in-flight data, then uses SQLite's `VACUUM INTO` to snapshot
  the index database and copies block files to the target directory.

  ## Parameters

    * `target_dir` - Directory to write backup files into (will be created)

  ## Returns

      {:ok, %{path: target_dir, files: [filenames], total_bytes: integer()}}

  ## Examples

      TimelessTraces.backup("/tmp/span_backup_2024")
  """
  @spec backup(String.t()) :: {:ok, map()} | {:error, term()}
  def backup(target_dir) do
    flush()

    File.mkdir_p!(target_dir)

    # Backup index DB via VACUUM INTO
    index_target = Path.join(target_dir, "index.db")

    case TimelessTraces.Index.backup(index_target) do
      :ok ->
        # Copy block files in parallel
        data_dir = TimelessTraces.Config.data_dir()
        blocks_src = Path.join(data_dir, "blocks")
        blocks_dst = Path.join(target_dir, "blocks")

        block_bytes = copy_block_files(blocks_src, blocks_dst)
        index_bytes = File.stat!(index_target).size

        {:ok,
         %{
           path: target_dir,
           files: ["index.db", "blocks"],
           total_bytes: index_bytes + block_bytes
         }}

      {:error, _} = err ->
        err
    end
  end

  defp copy_block_files(src_dir, dst_dir) do
    case File.ls(src_dir) do
      {:ok, files} ->
        File.mkdir_p!(dst_dir)

        files
        |> Task.async_stream(
          fn file ->
            src = Path.join(src_dir, file)
            dst = Path.join(dst_dir, file)
            File.cp!(src, dst)
            File.stat!(dst).size
          end,
          max_concurrency: System.schedulers_online()
        )
        |> Enum.reduce(0, fn {:ok, size}, acc -> acc + size end)

      {:error, :enoent} ->
        0
    end
  end
end
