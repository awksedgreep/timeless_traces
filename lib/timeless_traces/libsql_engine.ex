defmodule TimelessTraces.LibsqlEngine do
  @moduledoc """
  In-process storage engine over the timeless-libsql `timeless_traces`
  virtual table — the traces instance of the port pattern proven in
  timeless_logs (`timeless_logs/notes/libsql_engine_port_plan_2026-08-09.md`).

  Opt-in via `config :timeless_traces, engine: :libsql`. The writer owns
  `<data_dir>/traces.db`: ingest encodes the migration candidate's
  validated rich-span-v1 batches and control commands ride the vtab's
  command channel — the same public surface the external Rust owner
  uses, so embedded and external share one on-disk format.
  """

  use GenServer

  require Logger

  alias TimelessTraces.LibsqlCandidate

  @table "traces"
  @reader_pool_key {__MODULE__, :reader_pool}
  @columns "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,duration_ns," <>
             "attributes,status_description,events,resource,instrumentation_scope"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Ingest spans (maps or `%TimelessTraces.Span{}`)."
  def ingest([]), do: :ok
  def ingest(spans), do: GenServer.call(__MODULE__, {:ingest, spans}, call_timeout())

  @doc "Persist buffered spans into blocks now."
  def flush do
    case command("flush") do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc "Run a bounded background optimize pass."
  def optimize, do: command("optimize")

  @doc false
  def sql(sql, params \\ []), do: GenServer.call(__MODULE__, {:sql, sql, params}, call_timeout())

  @doc "Query spans with the facade's filter vocabulary."
  def query(filters), do: run_query(filters)

  @doc "All spans of one trace, ascending by start time."
  def trace(trace_id) do
    {clause, params} = trace_id_clause(trace_id, 1)

    with {:ok, rows} <-
           read(
             "SELECT #{@columns} FROM #{@table} WHERE #{clause} ORDER BY start_ts ASC",
             params
           ) do
      {:ok, rows |> Enum.map(&decode_row/1) |> Enum.map(&TimelessTraces.Span.from_map/1)}
    end
  end

  @doc "Distinct service names."
  def services do
    with {:ok, rows} <-
           read("SELECT DISTINCT service FROM #{@table} WHERE service != '' ORDER BY service") do
      {:ok, Enum.map(rows, fn [service] -> service end)}
    end
  end

  @doc "Distinct operation names for a service."
  def operations(service) do
    with {:ok, rows} <-
           read(
             "SELECT DISTINCT name FROM #{@table} WHERE service = ?1 ORDER BY name",
             [service]
           ) do
      {:ok, Enum.map(rows, fn [name] -> name end)}
    end
  end

  @doc "Aggregate statistics from timeless_stats('traces')."
  def stats do
    with {:ok, rows} <- read("SELECT * FROM timeless_stats('traces')") do
      kv = Map.new(rows, fn [key, value] -> {key, value} end)
      int = fn key -> stat_int(Map.get(kv, key)) end

      {:ok,
       %TimelessTraces.Stats{
         storage_mode: :libsql,
         total_blocks: int.("blocks") || 0,
         total_entries: int.("total_spans") || 0,
         total_bytes: int.("bytes_on_disk") || 0,
         disk_size: int.("bytes_on_disk") || 0,
         index_size: int.("index_bytes") || 0,
         raw_blocks: int.("raw_blocks") || 0,
         raw_bytes: int.("raw_bytes") || 0,
         compressed_blocks: int.("compressed_blocks") || 0,
         compressed_bytes: int.("compressed_bytes") || 0,
         compression_raw_bytes_in: int.("compression_input_bytes_total") || 0,
         compression_compressed_bytes_out: int.("compression_output_bytes_total") || 0,
         compaction_count: int.("optimize_count") || 0,
         oldest_timestamp: int.("ts_min"),
         newest_timestamp: int.("ts_max")
       }}
    end
  end

  @doc "Single-snapshot backup: flush, then VACUUM INTO <target>/traces.db."
  def backup(target_dir) do
    with :ok <- flush(),
         {path, extension_path} <- GenServer.call(__MODULE__, :connection_info, call_timeout()),
         :ok <- File.mkdir_p(target_dir),
         target = Path.join(target_dir, "traces.db"),
         {:ok, conn, _capabilities} <-
           LibsqlCandidate.open_readonly_connection(path, extension_path) do
      try do
        with {:ok, _} <- LibsqlCandidate.execute(conn, "VACUUM INTO ?1", [target]),
             {:ok, %{size: size}} <- File.stat(target) do
          {:ok, %{path: target_dir, files: ["traces.db"], total_bytes: size}}
        end
      after
        Exqlite.Sqlite3.close(conn)
      end
    end
  end

  defp command(cmd),
    do:
      GenServer.call(
        __MODULE__,
        {:sql, "INSERT INTO #{@table}(#{@table}) VALUES (?1)", [cmd]},
        :infinity
      )

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    data_dir = Keyword.get(opts, :data_dir, TimelessTraces.Config.data_dir())
    maybe_auto_migrate_legacy_store!(data_dir, opts)
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "traces.db")
    extension_path = Keyword.get(opts, :extension_path)

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessTraces.Config.retention_max_age())

    with {:ok, conn, capabilities} <-
           LibsqlCandidate.open_connection(path, extension_path),
         :ok <- LibsqlCandidate.initialize_database(conn, capabilities, retention_seconds),
         {:ok, readers} <- start_readers(path, extension_path, opts) do
      Logger.info(
        "timeless_traces libSQL engine: extension #{capabilities["extension_version"]} " <>
          "(data ABI #{capabilities["data_abi"]}) on #{path}"
      )

      install_reader_pool(readers)

      {:ok,
       %{
         conn: conn,
         path: path,
         extension_path: extension_path,
         readers: readers,
         flush_timer: schedule_flush()
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:ingest, spans}, _from, state) do
    result =
      with {:ok, blob} <- LibsqlCandidate.encode_batch(spans),
           {:ok, _} <-
             LibsqlCandidate.execute(
               state.conn,
               "INSERT INTO #{@table}(#{@table}) VALUES (?1)",
               [{:blob, blob}]
             ) do
        :ok
      end

    {:reply, result, state}
  end

  def handle_call({:sql, sql, params}, _from, state),
    do: {:reply, LibsqlCandidate.execute(state.conn, sql, params), state}

  def handle_call(:connection_info, _from, state),
    do: {:reply, {state.path, state.extension_path}, state}

  @impl true
  def handle_info(:flush, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    {:noreply, %{state | flush_timer: schedule_flush()}}
  end

  def handle_info({:EXIT, reader, _reason}, %{readers: readers} = state) do
    if reader in readers do
      case start_reader(state.path, state.extension_path) do
        {:ok, replacement} ->
          readers = Enum.map(readers, &if(&1 == reader, do: replacement, else: &1))
          install_reader_pool(readers)
          {:noreply, %{state | readers: readers}}

        {:error, reason} ->
          {:stop, {:reader_restart_failed, reason}, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase(@reader_pool_key)
    Enum.each(Map.get(state, :readers, []), &GenServer.stop(&1, :normal))
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    Exqlite.Sqlite3.close(state.conn)
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, TimelessTraces.Config.flush_interval())
  end

  defp start_readers(path, extension_path, opts) do
    count = Keyword.get(opts, :reader_pool_size, TimelessTraces.Config.libsql_reader_pool_size())

    if is_integer(count) and count > 0 do
      Enum.reduce_while(1..count, {:ok, []}, fn _index, {:ok, readers} ->
        case start_reader(path, extension_path) do
          {:ok, reader} -> {:cont, {:ok, [reader | readers]}}
          {:error, reason} -> {:halt, {:error, {:reader_start_failed, reason}}}
        end
      end)
      |> case do
        {:ok, readers} -> {:ok, Enum.reverse(readers)}
        error -> error
      end
    else
      {:error, {:invalid_reader_pool_size, count}}
    end
  end

  defp start_reader(path, extension_path) do
    TimelessTraces.LibsqlReader.start_link(path: path, extension_path: extension_path)
  end

  defp install_reader_pool(readers) do
    :persistent_term.put(@reader_pool_key, {List.to_tuple(readers), :atomics.new(1, [])})
  end

  defp read(sql, params \\ []) do
    reader = checkout_reader()
    TimelessTraces.LibsqlReader.execute(reader, sql, params, call_timeout())
  end

  defp read_transaction(statements) do
    reader = checkout_reader()
    TimelessTraces.LibsqlReader.transaction(reader, statements, call_timeout())
  end

  defp checkout_reader do
    {readers, cursor} = :persistent_term.get(@reader_pool_key)
    size = tuple_size(readers)
    index = rem(:atomics.add_get(cursor, 1, 1) - 1, size)
    elem(readers, index)
  end

  defp call_timeout, do: TimelessTraces.Config.query_timeout()

  # -- Query path -----------------------------------------------------------

  @pagination_keys [:limit, :offset, :order, :count_total]

  @kinds Map.new(~w(internal server client producer consumer), &{&1, String.to_atom(&1)})
  @statuses Map.new(~w(unset ok error), &{&1, String.to_atom(&1)})

  defp run_query(filters) do
    {pagination, search} = Enum.split_with(filters, fn {k, _} -> k in @pagination_keys end)
    order = Keyword.get(pagination, :order, :desc)
    limit = Keyword.get(pagination, :limit, 100)
    offset = Keyword.get(pagination, :offset, 0)
    count_total = Keyword.get(pagination, :count_total, true)
    {where_sql, params, residual} = sql_filters(search)

    if residual == [] do
      run_pushed_query(where_sql, params, order, limit, offset, count_total)
    else
      run_residual_query(where_sql, params, search, order, limit, offset)
    end
  end

  defp run_pushed_query(where_sql, params, order, limit, offset, true) do
    statements = [
      {"SELECT COUNT(*) FROM #{@table}#{where_sql}", params},
      {select_spans_sql(where_sql, order, limit, offset), params}
    ]

    with {:ok, [[[total]], rows]} <- read_transaction(statements) do
      entries = decode_spans(rows)

      {:ok,
       %TimelessTraces.Result{
         entries: entries,
         total: total,
         limit: limit,
         offset: offset,
         has_more: offset + length(entries) < total
       }}
    end
  end

  defp run_pushed_query(where_sql, params, order, limit, offset, false) do
    with {:ok, rows} <- select_spans(where_sql, params, order, limit + 1, offset) do
      has_more = length(rows) > limit
      entries = rows |> Enum.take(limit) |> decode_spans()
      total = offset + length(entries) + if(has_more, do: 1, else: 0)

      {:ok,
       %TimelessTraces.Result{
         entries: entries,
         total: total,
         limit: limit,
         offset: offset,
         has_more: has_more
       }}
    end
  end

  defp run_residual_query(where_sql, params, search, order, limit, offset) do
    with {:ok, rows} <- select_spans(where_sql, params, order) do
      matched = rows |> Enum.map(&decode_row/1) |> TimelessTraces.Filter.filter(search)
      total = length(matched)

      entries =
        matched
        |> Enum.drop(offset)
        |> Enum.take(limit)
        |> Enum.map(&TimelessTraces.Span.from_map/1)

      {:ok,
       %TimelessTraces.Result{
         entries: entries,
         total: total,
         limit: limit,
         offset: offset,
         has_more: offset + length(entries) < total
       }}
    end
  end

  defp sql_filters(search) do
    {where, params, residual} =
      Enum.reduce(search, {[], [], []}, fn
        {:since, ts}, {where, params, residual} ->
          {["start_ts >= ?#{length(params) + 1}" | where], params ++ [to_nanos(ts)], residual}

        {:until, ts}, {where, params, residual} ->
          {["start_ts <= ?#{length(params) + 1}" | where], params ++ [to_nanos(ts)], residual}

        {:min_duration, ns}, {where, params, residual} when is_integer(ns) ->
          {["duration_ns >= ?#{length(params) + 1}" | where], params ++ [ns], residual}

        {:max_duration, ns}, {where, params, residual} when is_integer(ns) ->
          {["duration_ns <= ?#{length(params) + 1}" | where], params ++ [ns], residual}

        {:service, service}, {where, params, residual} when is_binary(service) ->
          {["service = ?#{length(params) + 1}" | where], params ++ [service], residual}

        {:kind, kind}, {where, params, residual} when is_atom(kind) ->
          {["kind = ?#{length(params) + 1}" | where], params ++ [Atom.to_string(kind)], residual}

        {:status, status}, {where, params, residual} when is_atom(status) ->
          {["status = ?#{length(params) + 1}" | where], params ++ [Atom.to_string(status)],
           residual}

        {:trace_id, trace_id}, {where, params, residual} ->
          {clause, extra} = trace_id_clause(trace_id, length(params) + 1)
          {[clause | where], params ++ extra, residual}

        filter, {where, params, residual} ->
          {where, params, [filter | residual]}
      end)

    where_sql =
      if where == [], do: "", else: " WHERE " <> Enum.join(Enum.reverse(where), " AND ")

    {where_sql, params, Enum.reverse(residual)}
  end

  defp select_spans(where_sql, params, order, limit \\ nil, offset \\ 0) do
    read(select_spans_sql(where_sql, order, limit, offset), params)
  end

  defp select_spans_sql(where_sql, order, limit, offset) do
    order_sql = if order == :asc, do: " ORDER BY start_ts ASC", else: " ORDER BY start_ts DESC"

    page_sql =
      if is_integer(limit), do: " LIMIT #{max(limit, 0)} OFFSET #{max(offset, 0)}", else: ""

    "SELECT #{@columns} FROM #{@table}#{where_sql}#{order_sql}#{page_sql}"
  end

  defp decode_spans(rows) do
    rows |> Enum.map(&decode_row/1) |> Enum.map(&TimelessTraces.Span.from_map/1)
  end

  # Callers hold trace ids either as the raw 16 bytes (what the engine
  # returns) or as 32-char hex; match both without guessing.
  defp trace_id_clause(trace_id, first_param) do
    candidates =
      case trace_id do
        <<_::binary-size(32)>> = hex ->
          case Base.decode16(hex, case: :mixed) do
            {:ok, raw} -> [trace_id, raw]
            :error -> [trace_id]
          end

        _ ->
          [trace_id]
      end

    placeholders =
      candidates
      |> Enum.with_index(first_param)
      |> Enum.map_join(",", fn {_, i} -> "?#{i}" end)

    {"trace_id IN (#{placeholders})", Enum.map(candidates, &{:blob, &1})}
  end

  defp decode_row([tid, sid, parent, name, _service, kind, status, start, duration | rest]) do
    [attributes, description, events, resource, scope] = rest

    %{
      trace_id: hex_id(tid, 16),
      span_id: hex_id(sid, 8),
      parent_span_id: hex_id(parent, 8),
      name: name,
      kind: Map.get(@kinds, kind, :internal),
      start_time: start,
      end_time: start + (duration || 0),
      duration_ns: duration,
      status: Map.get(@statuses, status, :unset),
      status_message: if(description in [nil, ""], do: nil, else: description),
      attributes: decode_json_object(attributes),
      events: decode_json_array(events),
      resource: decode_json_object(resource),
      instrumentation_scope: decode_scope(scope)
    }
  end

  # Ids are stored as BLOBs — 16 bytes for a trace, 8 for a span — but the
  # public contract is lowercase hex, which is what the Elixir engine always
  # returned and what Index.trace/1 decodes back with Base.decode16!/2.
  # Handing the raw blob to callers renders as binary in the UI and makes any
  # lookup by id miss.
  #
  # The vtab accepts either form on input, so a value already stored as hex
  # text is passed through rather than double-encoded; the widths are fixed and
  # distinct, so the two cases cannot be confused.
  defp hex_id(nil, _raw_size), do: nil
  defp hex_id("", _raw_size), do: nil

  defp hex_id(value, raw_size) when is_binary(value) do
    case byte_size(value) do
      ^raw_size -> Base.encode16(value, case: :lower)
      _ -> String.downcase(value)
    end
  end

  defp hex_id(value, _raw_size), do: value

  defp decode_json_object(nil), do: %{}
  defp decode_json_object(""), do: %{}

  defp decode_json_object(json) when is_binary(json) do
    case :json.decode(json) do
      map when is_map(map) -> map
      _ -> %{}
    end
  rescue
    _ -> %{}
  end

  defp decode_json_array(nil), do: []
  defp decode_json_array(""), do: []

  defp decode_json_array(json) when is_binary(json) do
    case :json.decode(json) do
      list when is_list(list) -> list
      _ -> []
    end
  rescue
    _ -> []
  end

  # The candidate encodes a nil scope as {} — restore nil on read.
  defp decode_scope(scope) do
    case decode_json_object(scope) do
      map when map_size(map) == 0 -> nil
      map -> map
    end
  end

  defp to_nanos(%DateTime{} = dt), do: DateTime.to_unix(dt, :nanosecond)
  defp to_nanos(ts) when is_integer(ts), do: ts

  defp stat_int(nil), do: nil
  defp stat_int(""), do: nil
  defp stat_int(v) when is_integer(v), do: v

  defp stat_int(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, _} -> n
      :error -> nil
    end
  end

  defp stat_int(_), do: nil

  # A data_dir carrying the legacy block-store layout is AUTO-CONVERTED
  # at startup (the legacy engine is on a ~3-month deprecation clock):
  # ReleaseStartup.prepare/2 runs the journaled, resumable, digest-
  # verified conversion under an exclusive owner lock, retaining the
  # source for rollback. Set auto_migrate: false to restore the strict
  # refusal instead. Never silently ignore existing data.
  defp maybe_auto_migrate_legacy_store!(data_dir, opts) do
    legacy? =
      File.exists?(Path.join(data_dir, "traces_index.db")) or
        File.dir?(Path.join(data_dir, "blocks"))

    migrated? = File.exists?(Path.join(data_dir, "traces.db"))

    auto? =
      Keyword.get(
        opts,
        :auto_migrate,
        Application.get_env(:timeless_traces, :auto_migrate, true)
      )

    cond do
      not legacy? or migrated? ->
        :ok

      not auto? ->
        raise "timeless_traces engine: :libsql refuses to start against the unmigrated " <>
                "legacy block store in #{data_dir} — run the TimelessTraces.ReleaseMigration " <>
                "conversion, enable auto_migrate, or configure engine: :elixir"

      true ->
        Logger.warning(
          "timeless_traces: auto-converting the legacy block store in #{data_dir} to the " <>
            "libSQL engine (journaled, verified, source retained for rollback). " <>
            "Set auto_migrate: false to disable."
        )

        case TimelessTraces.ReleaseStartup.prepare(data_dir,
               extension_path: Keyword.get(opts, :extension_path)
             ) do
          {:ok, result} ->
            Logger.info(
              "timeless_traces: legacy conversion ready (state: #{inspect(result[:state])})"
            )

            :ok

          {:error, result} ->
            raise "timeless_traces: automatic legacy conversion failed: #{inspect(result)}. " <>
                    "The journaled migration is resumable — restart to resume, or set " <>
                    "engine: :elixir to keep the legacy engine."
        end
    end
  end
end
