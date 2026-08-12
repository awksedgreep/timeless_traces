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
  @columns "trace_id,span_id,parent_span_id,name,service,kind,status,start_ts,duration_ns," <>
             "attributes,status_description,events,resource,instrumentation_scope"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Ingest spans (maps or `%TimelessTraces.Span{}`)."
  def ingest([]), do: :ok
  def ingest(spans), do: GenServer.call(__MODULE__, {:ingest, spans}, :infinity)

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
  def sql(sql, params \\ []), do: GenServer.call(__MODULE__, {:sql, sql, params}, :infinity)

  @doc "Query spans with the facade's filter vocabulary."
  def query(filters), do: GenServer.call(__MODULE__, {:query, filters}, :infinity)

  @doc "All spans of one trace, ascending by start time."
  def trace(trace_id), do: GenServer.call(__MODULE__, {:trace, trace_id}, :infinity)

  @doc "Distinct service names."
  def services, do: GenServer.call(__MODULE__, :services, :infinity)

  @doc "Distinct operation names for a service."
  def operations(service), do: GenServer.call(__MODULE__, {:operations, service}, :infinity)

  @doc "Aggregate statistics from timeless_stats('traces')."
  def stats, do: GenServer.call(__MODULE__, :stats, :infinity)

  @doc "Single-snapshot backup: flush, then VACUUM INTO <target>/traces.db."
  def backup(target_dir), do: GenServer.call(__MODULE__, {:backup, target_dir}, :infinity)

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

    retention_seconds =
      Keyword.get(opts, :retention_seconds, TimelessTraces.Config.retention_max_age())

    with {:ok, conn, capabilities} <-
           LibsqlCandidate.open_connection(path, Keyword.get(opts, :extension_path)),
         :ok <- LibsqlCandidate.initialize_database(conn, capabilities, retention_seconds) do
      Logger.info(
        "timeless_traces libSQL engine: extension #{capabilities["extension_version"]} " <>
          "(data ABI #{capabilities["data_abi"]}) on #{path}"
      )

      {:ok, %{conn: conn, path: path, flush_timer: schedule_flush()}}
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

  def handle_call({:query, filters}, _from, state),
    do: {:reply, run_query(state.conn, filters), state}

  def handle_call({:trace, trace_id}, _from, state) do
    {clause, params} = trace_id_clause(trace_id, 1)

    result =
      with {:ok, rows} <-
             LibsqlCandidate.execute(
               state.conn,
               "SELECT #{@columns} FROM #{@table} WHERE #{clause} ORDER BY start_ts ASC",
               params
             ) do
        {:ok, rows |> Enum.map(&decode_row/1) |> Enum.map(&TimelessTraces.Span.from_map/1)}
      end

    {:reply, result, state}
  end

  def handle_call(:services, _from, state) do
    result =
      with {:ok, rows} <-
             LibsqlCandidate.execute(
               state.conn,
               "SELECT DISTINCT service FROM #{@table} WHERE service != '' ORDER BY service"
             ) do
        {:ok, Enum.map(rows, fn [s] -> s end)}
      end

    {:reply, result, state}
  end

  def handle_call({:operations, service}, _from, state) do
    result =
      with {:ok, rows} <-
             LibsqlCandidate.execute(
               state.conn,
               "SELECT DISTINCT name FROM #{@table} WHERE service = ?1 ORDER BY name",
               [service]
             ) do
        {:ok, Enum.map(rows, fn [n] -> n end)}
      end

    {:reply, result, state}
  end

  def handle_call(:stats, _from, state) do
    result =
      with {:ok, rows} <-
             LibsqlCandidate.execute(state.conn, "SELECT * FROM timeless_stats('traces')") do
        kv = Map.new(rows, fn [k, v] -> {k, v} end)
        int = fn key -> stat_int(Map.get(kv, key)) end

        {:ok,
         %TimelessTraces.Stats{
           total_blocks: int.("blocks") || 0,
           total_entries: int.("total_spans") || 0,
           total_bytes: int.("bytes_on_disk") || 0,
           disk_size: int.("bytes_on_disk") || 0,
           raw_blocks: int.("raw_blocks") || 0,
           raw_bytes: int.("raw_bytes") || 0,
           compressed_blocks: int.("compressed_blocks") || 0,
           compressed_bytes: int.("compressed_bytes") || 0,
           compression_raw_bytes_in: int.("optimize_raw_input_bytes") || 0,
           compression_compressed_bytes_out: int.("optimize_raw_output_bytes") || 0,
           compaction_count: int.("optimize_count") || 0,
           oldest_timestamp: int.("ts_min"),
           newest_timestamp: int.("ts_max")
         }}
      end

    {:reply, result, state}
  end

  def handle_call({:backup, target_dir}, _from, state) do
    result =
      with {:ok, _} <-
             LibsqlCandidate.execute(
               state.conn,
               "INSERT INTO #{@table}(#{@table}) VALUES ('flush')"
             ),
           :ok <- File.mkdir_p(target_dir),
           target = Path.join(target_dir, "traces.db"),
           {:ok, _} <- LibsqlCandidate.execute(state.conn, "VACUUM INTO ?1", [target]),
           {:ok, %{size: size}} <- File.stat(target) do
        {:ok, %{path: target_dir, files: ["traces.db"], total_bytes: size}}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info(:flush, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    {:noreply, %{state | flush_timer: schedule_flush()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _ = LibsqlCandidate.execute(state.conn, "INSERT INTO #{@table}(#{@table}) VALUES ('flush')")
    Exqlite.Sqlite3.close(state.conn)
  end

  defp schedule_flush do
    Process.send_after(self(), :flush, TimelessTraces.Config.flush_interval())
  end

  # -- Query path -----------------------------------------------------------

  @pagination_keys [:limit, :offset, :order, :count_total]

  @kinds Map.new(~w(internal server client producer consumer), &{&1, String.to_atom(&1)})
  @statuses Map.new(~w(unset ok error), &{&1, String.to_atom(&1)})

  defp run_query(conn, filters) do
    {pagination, search} = Enum.split_with(filters, fn {k, _} -> k in @pagination_keys end)
    # Traces default to newest-first, unlike logs.
    order = Keyword.get(pagination, :order, :desc)

    with {:ok, rows} <- select_spans(conn, search, order) do
      matched =
        rows
        |> Enum.map(&decode_row/1)
        |> TimelessTraces.Filter.filter(search)

      total = length(matched)
      limit = Keyword.get(pagination, :limit, 100)
      offset = Keyword.get(pagination, :offset, 0)

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
         has_more: offset + limit < total
       }}
    end
  end

  # service/kind/status/trace_id equality, the start_ts range, and the
  # duration range push into the vtab scan (block extrema + term/duration
  # pruning); the shared Filter re-checks everything, so pushdown is
  # purely an optimization.
  defp select_spans(conn, search, order) do
    {where, params} =
      Enum.reduce(search, {[], []}, fn
        {:since, ts}, {w, p} ->
          {["start_ts >= ?#{length(p) + 1}" | w], p ++ [to_nanos(ts)]}

        {:until, ts}, {w, p} ->
          {["start_ts <= ?#{length(p) + 1}" | w], p ++ [to_nanos(ts)]}

        {:min_duration, ns}, {w, p} when is_integer(ns) ->
          {["duration_ns >= ?#{length(p) + 1}" | w], p ++ [ns]}

        {:max_duration, ns}, {w, p} when is_integer(ns) ->
          {["duration_ns <= ?#{length(p) + 1}" | w], p ++ [ns]}

        {:service, service}, {w, p} when is_binary(service) ->
          {["service = ?#{length(p) + 1}" | w], p ++ [service]}

        {:kind, kind}, {w, p} when is_atom(kind) ->
          {["kind = ?#{length(p) + 1}" | w], p ++ [Atom.to_string(kind)]}

        {:status, status}, {w, p} when is_atom(status) ->
          {["status = ?#{length(p) + 1}" | w], p ++ [Atom.to_string(status)]}

        {:trace_id, trace_id}, {w, p} ->
          {clause, extra} = trace_id_clause(trace_id, length(p) + 1)
          {[clause | w], p ++ extra}

        _other, acc ->
          acc
      end)

    where_sql = if where == [], do: "", else: " WHERE " <> Enum.join(Enum.reverse(where), " AND ")
    order_sql = if order == :asc, do: " ORDER BY start_ts ASC", else: " ORDER BY start_ts DESC"

    LibsqlCandidate.execute(
      conn,
      "SELECT #{@columns} FROM #{@table}#{where_sql}#{order_sql}",
      params
    )
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
