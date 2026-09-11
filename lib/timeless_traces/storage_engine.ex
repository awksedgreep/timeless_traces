defmodule TimelessTraces.StorageEngine do
  @moduledoc false
  # Engine dispatch seam — the traces instance of the pattern proven in
  # timeless_metrics and timeless_logs. The supervisor records the
  # running engine in :persistent_term; the facade, exporter, and HTTP
  # ingest route through here so the deprecated Elixir block engine and
  # the libSQL engine stay swappable behind one public API.

  def engine, do: :persistent_term.get({TimelessTraces, :engine}, :elixir)

  @doc false
  def put_engine(engine) when engine in [:elixir, :libsql],
    do: :persistent_term.put({TimelessTraces, :engine}, engine)

  def ingest(spans) do
    case engine() do
      :libsql ->
        with :ok <- TimelessTraces.LibsqlEngine.ingest(spans) do
          TimelessTraces.Subscriber.broadcast(spans)
          :ok
        end

      _ ->
        TimelessTraces.Buffer.ingest(spans)
    end
  end

  def flush do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.flush()
      _ -> TimelessTraces.Buffer.flush()
    end
  end

  def query(filters) do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.query(filters)
      _ -> TimelessTraces.Index.query(filters)
    end
  end

  def trace(trace_id) do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.trace(trace_id)
      _ -> TimelessTraces.Index.trace(trace_id)
    end
  end

  def services do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.services()
      _ -> TimelessTraces.Index.distinct_services()
    end
  end

  def operations(service) do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.operations(service)
      _ -> TimelessTraces.Index.distinct_operations(service)
    end
  end

  def stats do
    case engine() do
      :libsql -> TimelessTraces.LibsqlEngine.stats()
      _ -> TimelessTraces.Index.stats()
    end
  end

  def merge_now do
    case engine() do
      :libsql ->
        case TimelessTraces.LibsqlEngine.optimize() do
          {:ok, _} -> :ok
          other -> other
        end

      _ ->
        TimelessTraces.Compactor.merge_now()
    end
  end
end
