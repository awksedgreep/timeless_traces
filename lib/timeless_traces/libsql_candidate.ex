defmodule TimelessTraces.LibsqlCandidate do
  @moduledoc false

  use GenServer

  @table "traces"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
  def sql(pid, sql, params \\ []), do: GenServer.call(pid, {:sql, sql, params}, :infinity)

  def checkpoint(pid, spans, journal, opts \\ []) do
    with {:ok, blob} <- encode_batch(spans) do
      GenServer.call(pid, {:checkpoint, blob, spans == [], journal, opts}, :infinity)
    end
  end

  def command(pid, command),
    do: sql(pid, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [command])

  def path(pid), do: GenServer.call(pid, :path)
  def phase(pid, phase), do: GenServer.call(pid, {:phase, phase}, :infinity)

  @doc false
  def open_connection(path, extension_path \\ nil) do
    open_connection(path, extension_path, [])
  end

  @doc false
  def open_readonly_connection(path, extension_path \\ nil) do
    open_connection(path, extension_path, mode: :readonly)
  end

  defp open_connection(path, extension_path, open_options) do
    extension = extension_path || extension_path!()

    case Exqlite.Sqlite3.open(path, open_options) do
      {:ok, conn} ->
        case load_and_verify(conn, extension) do
          {:ok, capabilities} ->
            {:ok, conn, capabilities}

          {:error, _} = error ->
            Exqlite.Sqlite3.close(conn)
            error
        end

      {:error, _} = error ->
        error
    end
  end

  defp load_and_verify(conn, extension) do
    with :ok <- load_extension(conn, extension),
         {:ok, capabilities} <- capabilities(conn),
         :ok <- require_capability(capabilities) do
      {:ok, capabilities}
    end
  end

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 closes the connection (and its WAL) even
    # when the linked caller dies mid-migration, not only on the clean
    # GenServer.stop path.
    Process.flag(:trap_exit, true)
    path = Keyword.fetch!(opts, :path)
    extension = Keyword.get(opts, :extension_path) || extension_path!()

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessTraces.Config.retention_max_age())

    File.mkdir_p!(Path.dirname(path))

    with :ok <- validate_retention(retention_seconds),
         {:ok, conn, capabilities} <- open_connection(path, extension),
         :ok <- initialize_database(conn, capabilities, retention_seconds) do
      {:ok, %{conn: conn, path: path}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:sql, sql, params}, _from, state),
    do: {:reply, execute(state.conn, sql, params), state}

  def handle_call(:path, _from, state), do: {:reply, state.path, state}

  def handle_call({:phase, phase}, _from, state) do
    now = System.system_time(:nanosecond)
    {:ok, _} = execute(state.conn, "BEGIN IMMEDIATE")

    result =
      with {:ok, _} <-
             execute(
               state.conn,
               "UPDATE _timeless_migration SET phase=?1,updated_at_ns=?2 WHERE singleton=1",
               [phase, now]
             ),
           {:ok, [[cursor, records]]} <-
             execute(
               state.conn,
               "SELECT cursor_json,records_completed FROM _timeless_migration WHERE singleton=1"
             ),
           {:ok, _} <-
             execute(
               state.conn,
               "INSERT INTO _timeless_migration_events(phase,cursor_json,records_completed,at_ns) VALUES (?1,?2,?3,?4)",
               [phase, cursor, records, now]
             ),
           {:ok, _} <- execute(state.conn, "COMMIT") do
        :ok
      else
        {:error, reason} ->
          _ = execute(state.conn, "ROLLBACK")
          {:error, reason}
      end

    {:reply, result, state}
  end

  def handle_call({:checkpoint, blob, empty?, journal, opts}, _from, state) do
    failpoint = Keyword.get(opts, :failpoint)
    final_page? = Keyword.get(opts, :final_page, false)
    {:ok, _} = execute(state.conn, "BEGIN IMMEDIATE")

    try do
      unless empty? do
        {:ok, _} =
          execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [{:blob, blob}])
      end

      if final_page? and not empty? do
        {:ok, _} = execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
      end

      failpoint!(failpoint, :disk_full)
      failpoint!(failpoint, :after_batch_before_journal)

      {:ok, _} =
        execute(
          state.conn,
          """
          UPDATE _timeless_migration
          SET phase=?1,cursor_json=?2,records_completed=?3,
              identity_digest=?4,relationship_digest=?5,
              updated_at_ns=?6,checkpoints=checkpoints+1
          WHERE singleton=1
          """,
          [
            journal.phase,
            journal.cursor_json,
            journal.records_completed,
            journal.identity_digest,
            journal.relationship_digest,
            journal.updated_at_ns
          ]
        )

      {:ok, _} =
        execute(
          state.conn,
          "INSERT INTO _timeless_migration_events(phase,cursor_json,records_completed,at_ns) VALUES (?1,?2,?3,?4)",
          [journal.phase, journal.cursor_json, journal.records_completed, journal.updated_at_ns]
        )

      failpoint!(failpoint, :after_journal_before_commit)
      {:ok, _} = execute(state.conn, "COMMIT")
      {:reply, :ok, state}
    rescue
      error ->
        _ = execute(state.conn, "ROLLBACK")
        {:reply, {:error, Exception.message(error)}, state}
    end
  end

  @impl true
  def terminate(_reason, state), do: Exqlite.Sqlite3.close(state.conn)

  @doc false
  def encode_batch(spans) do
    with {:ok, canonical} <- canonical_spans(spans) do
      count = length(canonical)
      header = <<0x02, 0, 0::little-16, count::little-32>>

      {:ok,
       IO.iodata_to_binary([
         header,
         Enum.map(canonical, & &1.trace_id),
         Enum.map(canonical, & &1.span_id),
         Enum.map(canonical, & &1.parent_span_id),
         text_column(canonical, & &1.name),
         text_column(canonical, & &1.service),
         Enum.map(canonical, &<<&1.kind>>),
         Enum.map(canonical, &<<&1.status>>),
         Enum.map(canonical, &<<&1.start_time::signed-little-64>>),
         Enum.map(canonical, &<<&1.duration_ns::signed-little-64>>),
         text_column(canonical, & &1.attributes),
         text_column(canonical, & &1.status_description),
         text_column(canonical, & &1.events),
         text_column(canonical, & &1.resource),
         text_column(canonical, & &1.instrumentation_scope)
       ])}
    end
  rescue
    error -> {:error, "cannot encode public rich traces batch: #{Exception.message(error)}"}
  end

  @doc false
  def canonical_span(span) do
    span =
      TimelessTraces.Span.from_map(if(is_struct(span), do: Map.from_struct(span), else: span))

    with {:ok, trace_id} <- id(span.trace_id, 16, :trace_id),
         {:ok, span_id} <- id(span.span_id, 8, :span_id),
         {:ok, parent_id} <- parent_id(span.parent_span_id),
         :ok <- consistent_duration(span),
         {:ok, attributes} <- json_object(span.attributes, :attributes),
         {:ok, events} <- json_array(span.events, :events),
         {:ok, resource} <- json_object(span.resource, :resource),
         {:ok, scope} <- json_object(span.instrumentation_scope || %{}, :instrumentation_scope) do
      {:ok,
       %{
         trace_id: trace_id,
         span_id: span_id,
         parent_span_id: parent_id,
         name: span.name || "",
         service: service(span.attributes, span.resource),
         kind: kind(span.kind),
         kind_name: Atom.to_string(span.kind),
         status: status(span.status),
         status_name: Atom.to_string(span.status),
         start_time: span.start_time,
         duration_ns: span.duration_ns,
         status_description: span.status_message || "",
         attributes: attributes,
         events: events,
         resource: resource,
         instrumentation_scope: scope
       }}
    end
  rescue
    error -> {:error, "invalid rich legacy span: #{Exception.message(error)}"}
  end

  defp canonical_spans(spans) do
    Enum.reduce_while(spans, {:ok, []}, fn span, {:ok, acc} ->
      case canonical_span(span) do
        {:ok, canonical} -> {:cont, {:ok, [canonical | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = error -> error
    end
  end

  defp id(value, bytes, _field) when is_binary(value) and byte_size(value) == bytes,
    do: {:ok, value}

  defp id(value, bytes, field) when is_binary(value) and byte_size(value) == bytes * 2 do
    case Base.decode16(value, case: :mixed) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> {:error, "#{field} is not #{bytes * 2} hexadecimal characters"}
    end
  end

  defp id(value, bytes, field),
    do: {:error, "#{field} must contain exactly #{bytes} bytes: #{inspect(value)}"}

  defp parent_id(nil), do: {:ok, <<0::64>>}
  defp parent_id(""), do: {:ok, <<0::64>>}
  defp parent_id(value), do: id(value, 8, :parent_span_id)

  defp consistent_duration(%{start_time: start, duration_ns: duration, end_time: finish})
       when is_integer(start) and is_integer(duration) and duration >= 0 do
    if is_nil(finish) or finish == start + duration do
      :ok
    else
      {:error, "end_time does not equal start_time + duration_ns"}
    end
  end

  defp consistent_duration(_),
    do: {:error, "start_time and non-negative duration_ns are required"}

  defp kind(:internal), do: 0
  defp kind(:server), do: 1
  defp kind(:client), do: 2
  defp kind(:producer), do: 3
  defp kind(:consumer), do: 4
  defp status(:unset), do: 0
  defp status(:ok), do: 1
  defp status(:error), do: 2

  defp service(attributes, resource) do
    case Map.get(attributes, "service.name") || Map.get(resource, "service.name") do
      value when is_binary(value) and value != "" -> value
      _ -> ""
    end
  end

  defp text_column(entries, mapper) do
    Enum.map(entries, fn entry ->
      value = mapper.(entry)
      [<<byte_size(value)::little-32>>, value]
    end)
  end

  defp json_object(value, field) when is_map(value) and not is_struct(value),
    do: json(value, field)

  defp json_object(_value, field), do: {:error, "#{field} must be a JSON object"}
  defp json_array(value, field) when is_list(value), do: json(value, field)
  defp json_array(_value, field), do: {:error, "#{field} must be a JSON array"}

  defp json(value, field) do
    {:ok, value |> json_value() |> :json.encode() |> IO.iodata_to_binary()}
  rescue
    error -> {:error, "#{field} is not canonical JSON: #{Exception.message(error)}"}
  end

  defp json_value(nil), do: :null
  defp json_value(value) when is_boolean(value), do: value
  defp json_value(value) when is_binary(value) or is_number(value), do: value
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)
  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)

  defp json_value(value) when is_map(value) and not is_struct(value),
    do: Map.new(value, fn {key, nested} -> {to_string(key), json_value(nested)} end)

  defp json_value(value), do: raise(ArgumentError, "unsupported JSON term #{inspect(value)}")

  def initialize_database(conn, capabilities, retention_seconds) do
    statements = [
      "PRAGMA page_size = 16384",
      "PRAGMA journal_mode = WAL",
      "PRAGMA synchronous = NORMAL",
      "PRAGMA auto_vacuum = INCREMENTAL",
      "PRAGMA busy_timeout = 5000",
      traces_create(retention_seconds),
      """
      CREATE TABLE IF NOT EXISTS _timeless_schema_migrations(
        signal TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version > 0),
        applied_at_unix INTEGER NOT NULL,
        server_version TEXT NOT NULL,
        extension_version TEXT NOT NULL,
        extension_data_abi INTEGER NOT NULL,
        PRIMARY KEY(signal,version)
      ) STRICT
      """
    ]

    with :ok <- execute_all(conn, statements) do
      version = Application.spec(:timeless_traces, :vsn) |> to_string()

      case execute(
             conn,
             "INSERT OR IGNORE INTO _timeless_schema_migrations VALUES ('traces',1,unixepoch(),?1,?2,?3)",
             [version, capabilities["extension_version"], capabilities["data_abi"]]
           ) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  defp traces_create(nil),
    do: "CREATE VIRTUAL TABLE IF NOT EXISTS traces USING timeless_traces"

  defp traces_create(seconds),
    do: "CREATE VIRTUAL TABLE IF NOT EXISTS traces USING timeless_traces(retention='#{seconds}s')"

  defp validate_retention(nil), do: :ok
  defp validate_retention(seconds) when is_integer(seconds) and seconds > 0, do: :ok

  defp validate_retention(value),
    do: {:error, "invalid traces retention seconds #{inspect(value)}"}

  defp execute_all(conn, statements) do
    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case execute(conn, sql) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp load_extension(conn, path) do
    with :ok <- Exqlite.Sqlite3.enable_load_extension(conn, true),
         {:ok, _} <- execute(conn, "SELECT load_extension(?1)", [path]),
         :ok <- Exqlite.Sqlite3.enable_load_extension(conn, false) do
      :ok
    end
  end

  defp capabilities(conn) do
    with {:ok, [[json]]} <- execute(conn, "SELECT timeless_capabilities()") do
      {:ok, :json.decode(json)}
    else
      other -> {:error, "extension capability handshake failed: #{inspect(other)}"}
    end
  end

  defp require_capability(capabilities) do
    batches = get_in(capabilities, ["signals", "traces", "batches"]) || []

    if capabilities["data_abi"] == 1 and "rich-span-v1" in batches do
      :ok
    else
      {:error, "extension lacks traces rich-span-v1/data ABI 1 capability"}
    end
  end

  def execute(conn, sql, params \\ []), do: TimelessTraces.DB.execute(conn, sql, params)

  defp failpoint!(configured, configured),
    do: raise("injected migration failure at #{configured}")

  defp failpoint!(_configured, _point), do: :ok

  defp extension_path! do
    System.get_env("TIMELESS_EXT_PATH") ||
      Application.get_env(:timeless_traces, :extension_path) ||
      raise "TIMELESS_EXT_PATH or :timeless_traces, :extension_path is required for libSQL migration"
  end
end
