defmodule TimelessTraces.LibsqlReader do
  @moduledoc false

  use GenServer

  alias TimelessTraces.LibsqlCandidate

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def execute(reader, sql, params, timeout) do
    GenServer.call(reader, {:execute, sql, params}, timeout)
  end

  def transaction(reader, statements, timeout) do
    GenServer.call(reader, {:transaction, statements}, timeout)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    extension_path = Keyword.get(opts, :extension_path)

    with {:ok, conn, _capabilities} <-
           LibsqlCandidate.open_readonly_connection(path, extension_path),
         :ok <- configure(conn) do
      {:ok, %{conn: conn}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:execute, sql, params}, _from, state) do
    {:reply, LibsqlCandidate.execute(state.conn, sql, params), state}
  end

  def handle_call({:transaction, statements}, _from, state) do
    {:ok, _} = LibsqlCandidate.execute(state.conn, "BEGIN")

    try do
      results =
        Enum.map(statements, fn {sql, params} ->
          {:ok, rows} = LibsqlCandidate.execute(state.conn, sql, params)
          rows
        end)

      {:ok, _} = LibsqlCandidate.execute(state.conn, "COMMIT")
      {:reply, {:ok, results}, state}
    rescue
      error ->
        _ = LibsqlCandidate.execute(state.conn, "ROLLBACK")
        reraise error, __STACKTRACE__
    end
  end

  @impl true
  def terminate(_reason, state), do: Exqlite.Sqlite3.close(state.conn)

  defp configure(conn) do
    statements = [
      "PRAGMA query_only = ON",
      "PRAGMA cache_size = #{TimelessTraces.Config.sqlite_reader_cache_size()}",
      "PRAGMA mmap_size = #{TimelessTraces.Config.sqlite_mmap_size()}",
      "PRAGMA temp_store = MEMORY",
      "PRAGMA busy_timeout = 5000"
    ]

    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case LibsqlCandidate.execute(conn, sql) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
