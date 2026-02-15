defmodule TimelessTraces.Index do
  @moduledoc false

  use GenServer

  @default_limit 100
  @default_offset 0

  # Flush pending index operations after this interval
  @index_flush_interval 100

  # Batch INSERT up to 400 terms per statement (800 params, under SQLite's 999 limit)
  @terms_batch_size 400

  # Batch INSERT OR REPLACE up to 100 traces per statement (6 params each = 600)
  @trace_batch_size 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec index_block(TimelessTraces.Writer.block_meta(), [map()]) :: :ok
  def index_block(block_meta, entries) do
    {terms, trace_rows} = precompute(entries)
    GenServer.call(__MODULE__, {:index_block, block_meta, terms, trace_rows})
  end

  @spec index_block_async(TimelessTraces.Writer.block_meta(), [map()]) :: :ok
  def index_block_async(block_meta, entries) do
    {terms, trace_rows} = precompute(entries)
    GenServer.cast(__MODULE__, {:index_block, block_meta, terms, trace_rows})
  end

  @spec query(keyword()) :: {:ok, TimelessTraces.Result.t()}
  def query(filters) do
    # Phase 1: GenServer does the cheap SQLite lookup only
    {block_ids, storage, pagination, search_filters} =
      GenServer.call(__MODULE__, {:query_plan, filters}, TimelessTraces.Config.query_timeout())

    # Phase 2: Parallel decompression + filtering in the caller's process
    do_query_parallel(block_ids, storage, pagination, search_filters)
  end

  @spec trace(String.t()) :: {:ok, [TimelessTraces.Span.t()]}
  def trace(trace_id) do
    # Phase 1: GenServer does the cheap SQLite lookup only
    {block_info, storage} =
      GenServer.call(__MODULE__, {:trace_plan, trace_id}, TimelessTraces.Config.query_timeout())

    # Phase 2: Parallel decompression in the caller's process
    do_trace_parallel(block_info, storage, trace_id)
  end

  @spec delete_blocks_before(integer()) :: non_neg_integer()
  def delete_blocks_before(cutoff_timestamp) do
    GenServer.call(__MODULE__, {:delete_before, cutoff_timestamp}, 60_000)
  end

  @spec delete_blocks_over_size(non_neg_integer()) :: non_neg_integer()
  def delete_blocks_over_size(max_bytes) do
    GenServer.call(__MODULE__, {:delete_over_size, max_bytes}, 60_000)
  end

  @spec stats() :: {:ok, TimelessTraces.Stats.t()}
  def stats do
    GenServer.call(__MODULE__, :stats, TimelessTraces.Config.query_timeout())
  end

  @spec matching_block_ids(keyword()) :: [{integer(), String.t() | nil, :raw | :zstd}]
  def matching_block_ids(filters) do
    GenServer.call(
      __MODULE__,
      {:matching_block_ids, filters},
      TimelessTraces.Config.query_timeout()
    )
  end

  @spec read_block_data(integer()) :: {:ok, [map()]} | {:error, term()}
  def read_block_data(block_id) do
    GenServer.call(
      __MODULE__,
      {:read_block_data, block_id},
      TimelessTraces.Config.query_timeout()
    )
  end

  @spec raw_block_stats() :: %{
          entry_count: integer(),
          block_count: integer(),
          oldest_created_at: integer() | nil
        }
  def raw_block_stats do
    GenServer.call(__MODULE__, :raw_block_stats, TimelessTraces.Config.query_timeout())
  end

  @spec raw_block_ids() :: [{integer(), String.t() | nil}]
  def raw_block_ids do
    GenServer.call(__MODULE__, :raw_block_ids, TimelessTraces.Config.query_timeout())
  end

  @spec compact_blocks([integer()], TimelessTraces.Writer.block_meta(), [map()]) :: :ok
  def compact_blocks(old_block_ids, new_meta, new_entries) do
    {terms, trace_rows} = precompute(new_entries)

    GenServer.call(
      __MODULE__,
      {:compact_blocks, old_block_ids, new_meta, terms, trace_rows},
      60_000
    )
  end

  @spec backup(String.t()) :: :ok | {:error, term()}
  def backup(target_path) do
    GenServer.call(__MODULE__, {:backup, target_path}, :infinity)
  end

  @spec distinct_services() :: {:ok, [String.t()]}
  def distinct_services do
    GenServer.call(__MODULE__, :distinct_services, TimelessTraces.Config.query_timeout())
  end

  @spec distinct_operations(String.t()) :: {:ok, [String.t()]}
  def distinct_operations(service) do
    GenServer.call(
      __MODULE__,
      {:distinct_operations, service},
      TimelessTraces.Config.query_timeout()
    )
  end

  @doc false
  @spec precompute([map()]) :: {[String.t()], [tuple()]}
  def precompute(entries) do
    terms = extract_terms(entries)
    trace_rows = compute_trace_rows(entries)
    {terms, trace_rows}
  end

  # --- GenServer callbacks ---

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, :disk)

    {db, db_path} =
      case storage do
        :memory ->
          {:ok, db} = Exqlite.Sqlite3.open(":memory:")
          {db, nil}

        :disk ->
          data_dir = Keyword.fetch!(opts, :data_dir)
          db_path = Path.join(data_dir, "index.db")
          {:ok, db} = Exqlite.Sqlite3.open(db_path)
          {db, db_path}
      end

    create_tables(db)

    # Cache prepared statements for hot-path inserts
    {:ok, block_insert_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      INSERT INTO blocks (block_id, file_path, byte_size, entry_count, ts_min, ts_max, data, format)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      """)

    {:ok, terms_insert_stmt} = Exqlite.Sqlite3.prepare(db, build_terms_sql(@terms_batch_size))
    {:ok, trace_insert_stmt} = Exqlite.Sqlite3.prepare(db, build_trace_sql(@trace_batch_size))

    {:ok,
     %{
       db: db,
       db_path: db_path,
       storage: storage,
       pending: [],
       flush_timer: nil,
       block_insert_stmt: block_insert_stmt,
       terms_insert_stmt: terms_insert_stmt,
       trace_insert_stmt: trace_insert_stmt
     }}
  end

  @impl true
  def terminate(_reason, state) do
    flush_pending(state)
    Exqlite.Sqlite3.release(state.db, state.block_insert_stmt)
    Exqlite.Sqlite3.release(state.db, state.terms_insert_stmt)
    Exqlite.Sqlite3.release(state.db, state.trace_insert_stmt)
    Exqlite.Sqlite3.close(state.db)
  end

  # --- handle_call (grouped) ---

  @impl true
  def handle_call({:index_block, meta, terms, trace_rows}, _from, state) do
    state = flush_pending(state)
    result = do_index_block(state, meta, terms, trace_rows)
    {:reply, result, state}
  end

  def handle_call({:query_plan, filters}, _from, state) do
    state = flush_pending(state)
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :desc)
    block_ids = find_matching_blocks(state.db, term_filters, time_filters, order)
    {:reply, {block_ids, state.storage, pagination, search_filters}, state}
  end

  def handle_call({:trace_plan, trace_id}, _from, state) do
    state = flush_pending(state)
    block_info = find_trace_blocks(state.db, trace_id)
    {:reply, {block_info, state.storage}, state}
  end

  def handle_call({:delete_before, cutoff}, _from, state) do
    state = flush_pending(state)
    count = do_delete_before(state.db, cutoff, state.storage)
    {:reply, count, state}
  end

  def handle_call({:delete_over_size, max_bytes}, _from, state) do
    state = flush_pending(state)
    count = do_delete_over_size(state.db, max_bytes, state.storage)
    {:reply, count, state}
  end

  def handle_call({:matching_block_ids, filters}, _from, state) do
    state = flush_pending(state)
    {search_filters, pagination} = split_pagination(filters)
    {term_filters, time_filters} = split_filters(search_filters)
    order = Keyword.get(pagination, :order, :asc)
    block_ids = find_matching_blocks(state.db, term_filters, time_filters, order)
    {:reply, block_ids, state}
  end

  def handle_call(:stats, _from, state) do
    state = flush_pending(state)
    result = do_stats(state.db, state.db_path)
    {:reply, result, state}
  end

  def handle_call({:read_block_data, block_id}, _from, state) do
    state = flush_pending(state)
    result = read_block_from_db(state.db, block_id)
    {:reply, result, state}
  end

  def handle_call(:raw_block_stats, _from, state) do
    state = flush_pending(state)
    result = do_raw_block_stats(state.db)
    {:reply, result, state}
  end

  def handle_call(:raw_block_ids, _from, state) do
    state = flush_pending(state)
    result = do_raw_block_ids(state.db)
    {:reply, result, state}
  end

  def handle_call({:compact_blocks, old_ids, new_meta, terms, trace_rows}, _from, state) do
    state = flush_pending(state)
    result = do_compact_blocks(state, old_ids, new_meta, terms, trace_rows)
    {:reply, result, state}
  end

  def handle_call({:backup, target_path}, _from, state) do
    state = flush_pending(state)
    {:ok, stmt} = Exqlite.Sqlite3.prepare(state.db, "VACUUM INTO ?1")
    Exqlite.Sqlite3.bind(stmt, [target_path])
    result = Exqlite.Sqlite3.step(state.db, stmt)
    Exqlite.Sqlite3.release(state.db, stmt)

    case result do
      :done -> {:reply, :ok, state}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  def handle_call(:distinct_services, _from, state) do
    state = flush_pending(state)
    result = do_distinct_services(state.db)
    {:reply, result, state}
  end

  def handle_call({:distinct_operations, service}, _from, state) do
    state = flush_pending(state)
    result = do_distinct_operations(state.db, service)
    {:reply, result, state}
  end

  # --- handle_cast ---

  @impl true
  def handle_cast({:index_block, meta, terms, trace_rows}, state) do
    pending = [{meta, terms, trace_rows} | state.pending]
    state = schedule_index_flush(%{state | pending: pending})
    {:noreply, state}
  end

  # --- handle_info ---

  @impl true
  def handle_info(:flush_index, state) do
    state = %{state | flush_timer: nil}
    state = flush_pending(state)
    {:noreply, state}
  end

  # --- Pending flush helpers ---

  defp flush_pending(%{pending: []} = state), do: state

  defp flush_pending(%{pending: pending} = state) do
    Exqlite.Sqlite3.execute(state.db, "BEGIN")

    for {meta, terms, trace_rows} <- Enum.reverse(pending) do
      insert_block_data(state, meta, terms, trace_rows)
    end

    Exqlite.Sqlite3.execute(state.db, "COMMIT")

    if state.flush_timer do
      Process.cancel_timer(state.flush_timer)
    end

    %{state | pending: [], flush_timer: nil}
  end

  defp schedule_index_flush(%{flush_timer: nil} = state) do
    ref = Process.send_after(self(), :flush_index, @index_flush_interval)
    %{state | flush_timer: ref}
  end

  defp schedule_index_flush(state), do: state

  # --- Table creation ---

  defp create_tables(db) do
    Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")
    Exqlite.Sqlite3.execute(db, "PRAGMA cache_size = -16000")
    Exqlite.Sqlite3.execute(db, "PRAGMA mmap_size = 134217728")
    Exqlite.Sqlite3.execute(db, "PRAGMA temp_store = memory")

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS blocks (
      block_id INTEGER PRIMARY KEY,
      file_path TEXT,
      byte_size INTEGER NOT NULL,
      entry_count INTEGER NOT NULL,
      ts_min INTEGER NOT NULL,
      ts_max INTEGER NOT NULL,
      data BLOB,
      format TEXT NOT NULL DEFAULT 'zstd',
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    )
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS block_terms (
      term TEXT NOT NULL,
      block_id INTEGER NOT NULL REFERENCES blocks(block_id),
      PRIMARY KEY (term, block_id)
    ) WITHOUT ROWID
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE TABLE IF NOT EXISTS trace_index (
      trace_id TEXT NOT NULL,
      block_id INTEGER NOT NULL REFERENCES blocks(block_id),
      span_count INTEGER NOT NULL,
      root_span_name TEXT,
      duration_ns INTEGER,
      has_error INTEGER DEFAULT 0,
      PRIMARY KEY (trace_id, block_id)
    ) WITHOUT ROWID
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE INDEX IF NOT EXISTS idx_blocks_ts ON blocks(ts_min, ts_max)
    """)

    Exqlite.Sqlite3.execute(db, """
    CREATE INDEX IF NOT EXISTS idx_trace_error ON trace_index(has_error)
    """)
  end

  # --- Indexing ---

  defp do_index_block(state, meta, terms, trace_rows) do
    Exqlite.Sqlite3.execute(state.db, "BEGIN")
    insert_block_data(state, meta, terms, trace_rows)
    Exqlite.Sqlite3.execute(state.db, "COMMIT")
    :ok
  end

  # Insert pre-computed block data (caller manages transaction)
  defp insert_block_data(state, meta, terms, trace_rows) do
    format = Map.get(meta, :format, :zstd)
    format_str = Atom.to_string(format)
    db = state.db

    Exqlite.Sqlite3.bind(state.block_insert_stmt, [
      meta.block_id,
      meta[:file_path],
      meta.byte_size,
      meta.entry_count,
      meta.ts_min,
      meta.ts_max,
      meta[:data],
      format_str
    ])

    Exqlite.Sqlite3.step(db, state.block_insert_stmt)
    Exqlite.Sqlite3.reset(state.block_insert_stmt)

    insert_terms_batch(state, terms, meta.block_id)
    insert_trace_batch(state, meta.block_id, trace_rows)
  end

  defp insert_terms_batch(_state, [], _block_id), do: :ok

  defp insert_terms_batch(state, terms, block_id) do
    db = state.db

    terms
    |> Enum.chunk_every(@terms_batch_size)
    |> Enum.each(fn batch ->
      n = length(batch)
      params = Enum.flat_map(batch, fn term -> [term, block_id] end)

      if n == @terms_batch_size do
        Exqlite.Sqlite3.bind(state.terms_insert_stmt, params)
        Exqlite.Sqlite3.step(db, state.terms_insert_stmt)
        Exqlite.Sqlite3.reset(state.terms_insert_stmt)
      else
        sql = build_terms_sql(n)
        {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)
        Exqlite.Sqlite3.bind(stmt, params)
        Exqlite.Sqlite3.step(db, stmt)
        Exqlite.Sqlite3.release(db, stmt)
      end
    end)
  end

  defp insert_trace_batch(_state, _block_id, []), do: :ok

  defp insert_trace_batch(state, block_id, trace_rows) do
    db = state.db

    trace_rows
    |> Enum.chunk_every(@trace_batch_size)
    |> Enum.each(fn batch ->
      n = length(batch)

      params =
        Enum.flat_map(batch, fn {trace_id, count, root_name, dur, err} ->
          [trace_id, block_id, count, root_name, dur, err]
        end)

      if n == @trace_batch_size do
        Exqlite.Sqlite3.bind(state.trace_insert_stmt, params)
        Exqlite.Sqlite3.step(db, state.trace_insert_stmt)
        Exqlite.Sqlite3.reset(state.trace_insert_stmt)
      else
        sql = build_trace_sql(n)
        {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)
        Exqlite.Sqlite3.bind(stmt, params)
        Exqlite.Sqlite3.step(db, stmt)
        Exqlite.Sqlite3.release(db, stmt)
      end
    end)
  end

  defp extract_terms(entries) do
    entries
    |> Enum.flat_map(fn span ->
      base_terms = [
        "name:#{span.name}",
        "kind:#{span.kind}",
        "status:#{span.status}"
      ]

      service_term =
        case Map.get(span.attributes, "service.name") ||
               Map.get(span.resource || %{}, "service.name") do
          nil -> []
          svc -> ["service.name:#{svc}"]
        end

      attr_terms =
        (span.attributes || %{})
        |> Enum.flat_map(fn
          {"http.method", v} -> ["http.method:#{v}"]
          {"http.status_code", v} -> ["http.status_code:#{v}"]
          {"http.route", v} -> ["http.route:#{v}"]
          {"db.system", v} -> ["db.system:#{v}"]
          {"rpc.system", v} -> ["rpc.system:#{v}"]
          {"messaging.system", v} -> ["messaging.system:#{v}"]
          _ -> []
        end)

      base_terms ++ service_term ++ attr_terms
    end)
    |> Enum.uniq()
  end

  defp compute_trace_rows(entries) do
    by_trace = Enum.group_by(entries, & &1.trace_id)

    Enum.map(by_trace, fn {trace_id, spans} ->
      root = Enum.find(spans, fn s -> s.parent_span_id == nil end) || hd(spans)
      has_error = if Enum.any?(spans, fn s -> s.status == :error end), do: 1, else: 0

      {min_start, max_end, count} =
        Enum.reduce(spans, {nil, nil, 0}, fn s, {mn, mx, c} ->
          {
            if(mn == nil or s.start_time < mn, do: s.start_time, else: mn),
            if(mx == nil or s.end_time > mx, do: s.end_time, else: mx),
            c + 1
          }
        end)

      duration_ns = max_end - min_start
      {trace_id, count, to_string(root.name), duration_ns, has_error}
    end)
  end

  defp build_terms_sql(n) do
    placeholders =
      Enum.map_join(1..n, ", ", fn i ->
        "(?#{i * 2 - 1}, ?#{i * 2})"
      end)

    "INSERT OR IGNORE INTO block_terms (term, block_id) VALUES #{placeholders}"
  end

  defp build_trace_sql(n) do
    placeholders =
      Enum.map_join(1..n, ", ", fn i ->
        base = (i - 1) * 6
        "(?#{base + 1}, ?#{base + 2}, ?#{base + 3}, ?#{base + 4}, ?#{base + 5}, ?#{base + 6})"
      end)

    "INSERT OR REPLACE INTO trace_index (trace_id, block_id, span_count, root_span_name, duration_ns, has_error) VALUES #{placeholders}"
  end

  # --- Querying (parallel, runs in caller's process) ---

  defp do_query_parallel(block_ids, storage, pagination, search_filters) do
    start_time = System.monotonic_time()

    limit = Keyword.get(pagination, :limit, @default_limit)
    offset = Keyword.get(pagination, :offset, @default_offset)
    order = Keyword.get(pagination, :order, :desc)
    blocks_read = length(block_ids)

    all_matching =
      if storage == :disk and blocks_read > 1 do
        block_ids
        |> Task.async_stream(
          fn {_block_id, file_path, format} ->
            format_atom = to_format_atom(format)

            case TimelessTraces.Writer.read_block(file_path, format_atom) do
              {:ok, entries} ->
                entries
                |> TimelessTraces.Filter.filter(search_filters)
                |> Enum.map(&TimelessTraces.Span.from_map/1)

              {:error, reason} ->
                TimelessTraces.Telemetry.event(
                  [:timeless_traces, :block, :error],
                  %{},
                  %{file_path: file_path, reason: reason}
                )

                []
            end
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
      else
        Enum.flat_map(block_ids, fn {block_id, file_path, format} ->
          format_atom = to_format_atom(format)

          read_result =
            case storage do
              :disk -> TimelessTraces.Writer.read_block(file_path, format_atom)
              :memory -> read_block_data(block_id)
            end

          case read_result do
            {:ok, entries} ->
              entries
              |> TimelessTraces.Filter.filter(search_filters)
              |> Enum.map(&TimelessTraces.Span.from_map/1)

            {:error, reason} ->
              TimelessTraces.Telemetry.event(
                [:timeless_traces, :block, :error],
                %{},
                %{file_path: file_path, reason: reason}
              )

              []
          end
        end)
      end

    sorted =
      case order do
        :asc -> Enum.sort_by(all_matching, & &1.start_time, :asc)
        :desc -> Enum.sort_by(all_matching, & &1.start_time, :desc)
      end

    total = length(sorted)
    page = sorted |> Enum.drop(offset) |> Enum.take(limit)
    duration = System.monotonic_time() - start_time

    TimelessTraces.Telemetry.event(
      [:timeless_traces, :query, :stop],
      %{duration: duration, total: total, blocks_read: blocks_read},
      %{filters: search_filters}
    )

    {:ok,
     %TimelessTraces.Result{
       entries: page,
       total: total,
       limit: limit,
       offset: offset
     }}
  end

  defp find_trace_blocks(db, trace_id) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT ti.block_id, b.file_path, b.format
      FROM trace_index ti
      JOIN blocks b ON ti.block_id = b.block_id
      WHERE ti.trace_id = ?1
      """)

    Exqlite.Sqlite3.bind(stmt, [trace_id])
    rows = collect_rows_3(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    rows
  end

  defp do_trace_parallel(block_info, storage, trace_id) do
    spans =
      if storage == :disk and length(block_info) > 1 do
        block_info
        |> Task.async_stream(
          fn {_block_id, file_path, format} ->
            format_atom = to_format_atom(format)

            case TimelessTraces.Writer.read_block(file_path, format_atom) do
              {:ok, entries} ->
                entries
                |> Enum.filter(fn e -> e.trace_id == trace_id end)
                |> Enum.map(&TimelessTraces.Span.from_map/1)

              {:error, _} ->
                []
            end
          end,
          max_concurrency: System.schedulers_online(),
          ordered: false
        )
        |> Enum.flat_map(fn {:ok, entries} -> entries end)
      else
        Enum.flat_map(block_info, fn {block_id, file_path, format} ->
          format_atom = to_format_atom(format)

          read_result =
            case storage do
              :disk -> TimelessTraces.Writer.read_block(file_path, format_atom)
              :memory -> read_block_data(block_id)
            end

          case read_result do
            {:ok, entries} ->
              entries
              |> Enum.filter(fn e -> e.trace_id == trace_id end)
              |> Enum.map(&TimelessTraces.Span.from_map/1)

            {:error, _} ->
              []
          end
        end)
      end

    {:ok, Enum.sort_by(spans, & &1.start_time)}
  end

  # --- Stats ---

  defp do_stats(db, db_path) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT
        COUNT(*),
        COALESCE(SUM(entry_count), 0),
        COALESCE(SUM(byte_size), 0),
        MIN(ts_min),
        MAX(ts_max)
      FROM blocks
      """)

    {:row, [total_blocks, total_entries, total_bytes, oldest, newest]} =
      Exqlite.Sqlite3.step(db, stmt)

    Exqlite.Sqlite3.release(db, stmt)

    {:ok, fmt_stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT format, COUNT(*), COALESCE(SUM(byte_size), 0), COALESCE(SUM(entry_count), 0)
      FROM blocks GROUP BY format
      """)

    format_stats = collect_format_stats(db, fmt_stmt)
    Exqlite.Sqlite3.release(db, fmt_stmt)

    index_size =
      case db_path do
        nil ->
          0

        path ->
          case File.stat(path) do
            {:ok, %{size: size}} -> size
            _ -> 0
          end
      end

    {:ok,
     %TimelessTraces.Stats{
       total_blocks: total_blocks,
       total_entries: total_entries,
       total_bytes: total_bytes,
       oldest_timestamp: oldest,
       newest_timestamp: newest,
       disk_size: total_bytes,
       index_size: index_size,
       raw_blocks: format_stats["raw"][:blocks] || 0,
       raw_bytes: format_stats["raw"][:bytes] || 0,
       raw_entries: format_stats["raw"][:entries] || 0,
       zstd_blocks: format_stats["zstd"][:blocks] || 0,
       zstd_bytes: format_stats["zstd"][:bytes] || 0,
       zstd_entries: format_stats["zstd"][:entries] || 0
     }}
  end

  defp collect_format_stats(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [format, count, bytes, entries]} ->
        Map.put(collect_format_stats(db, stmt), format, %{
          blocks: count,
          bytes: bytes,
          entries: entries
        })

      :done ->
        %{}
    end
  end

  # --- Block reading ---

  defp read_block_from_db(db, block_id) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT data, format FROM blocks WHERE block_id = ?1")

    Exqlite.Sqlite3.bind(stmt, [block_id])

    result =
      case Exqlite.Sqlite3.step(db, stmt) do
        {:row, [data, format]} when is_binary(data) ->
          TimelessTraces.Writer.decompress_block(data, to_format_atom(format))

        {:row, [nil, _format]} ->
          {:error, :no_data}

        :done ->
          {:error, :not_found}
      end

    Exqlite.Sqlite3.release(db, stmt)
    result
  end

  # --- Raw block helpers ---

  defp do_raw_block_stats(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT
        COALESCE(SUM(entry_count), 0),
        COUNT(*),
        MIN(created_at)
      FROM blocks WHERE format = 'raw'
      """)

    {:row, [entry_count, block_count, oldest]} = Exqlite.Sqlite3.step(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    %{entry_count: entry_count, block_count: block_count, oldest_created_at: oldest}
  end

  defp do_raw_block_ids(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT block_id, file_path FROM blocks WHERE format = 'raw' ORDER BY ts_min ASC
      """)

    rows = collect_rows_2(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    rows
  end

  # --- Compaction ---

  defp do_compact_blocks(state, old_ids, new_meta, terms, trace_rows) do
    db = state.db
    storage = state.storage

    old_file_paths =
      if storage == :disk do
        Enum.flat_map(old_ids, fn id ->
          {:ok, stmt} =
            Exqlite.Sqlite3.prepare(db, "SELECT file_path FROM blocks WHERE block_id = ?1")

          Exqlite.Sqlite3.bind(stmt, [id])

          result =
            case Exqlite.Sqlite3.step(db, stmt) do
              {:row, [path]} when is_binary(path) -> [path]
              _ -> []
            end

          Exqlite.Sqlite3.release(db, stmt)
          result
        end)
      else
        []
      end

    Exqlite.Sqlite3.execute(db, "BEGIN")

    for id <- old_ids do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM trace_index WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)
    end

    insert_block_data(state, new_meta, terms, trace_rows)

    Exqlite.Sqlite3.execute(db, "COMMIT")

    for path <- old_file_paths, do: File.rm(path)

    :ok
  end

  # --- Deletion ---

  defp do_delete_before(db, cutoff_timestamp, storage) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, "SELECT block_id, file_path FROM blocks WHERE ts_max < ?1")

    Exqlite.Sqlite3.bind(stmt, [cutoff_timestamp])
    blocks = collect_rows_2(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    delete_blocks(db, blocks, storage)
  end

  defp do_delete_over_size(db, max_bytes, storage) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "SELECT COALESCE(SUM(byte_size), 0) FROM blocks")
    {:row, [total]} = Exqlite.Sqlite3.step(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)

    if total <= max_bytes do
      0
    else
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(
          db,
          "SELECT block_id, file_path, byte_size FROM blocks ORDER BY ts_min ASC"
        )

      rows = collect_rows_with_size(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {to_delete, _} =
        Enum.reduce_while(rows, {[], total}, fn {block_id, file_path, size}, {acc, remaining} ->
          if remaining > max_bytes do
            {:cont, {[{block_id, file_path} | acc], remaining - size}}
          else
            {:halt, {acc, remaining}}
          end
        end)

      delete_blocks(db, to_delete, storage)
    end
  end

  defp delete_blocks(_db, [], _storage), do: 0

  defp delete_blocks(db, blocks, storage) do
    Exqlite.Sqlite3.execute(db, "BEGIN")

    for {block_id, file_path} <- blocks do
      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM block_terms WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [block_id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM trace_index WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [block_id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      {:ok, stmt} = Exqlite.Sqlite3.prepare(db, "DELETE FROM blocks WHERE block_id = ?1")
      Exqlite.Sqlite3.bind(stmt, [block_id])
      Exqlite.Sqlite3.step(db, stmt)
      Exqlite.Sqlite3.release(db, stmt)

      if storage == :disk do
        File.rm(file_path)
      end
    end

    Exqlite.Sqlite3.execute(db, "COMMIT")
    length(blocks)
  end

  # --- Query building ---

  defp split_pagination(filters) do
    {pagination, search} =
      Enum.split_with(filters, fn {k, _v} -> k in [:limit, :offset, :order] end)

    {search, pagination}
  end

  defp split_filters(filters) do
    term_filters =
      Enum.filter(filters, fn {k, _v} ->
        k in [:name, :kind, :status, :service, :attributes]
      end)

    time_filters =
      Enum.filter(filters, fn {k, _v} -> k in [:since, :until] end)

    {term_filters, time_filters}
  end

  defp find_matching_blocks(db, term_filters, time_filters, order) do
    terms = build_query_terms(term_filters)

    {where_clauses, params} = build_where(terms, time_filters)

    order_dir = if order == :asc, do: "ASC", else: "DESC"

    sql =
      if where_clauses == [] do
        "SELECT block_id, file_path, format FROM blocks ORDER BY ts_min #{order_dir}"
      else
        """
        SELECT DISTINCT b.block_id, b.file_path, b.format FROM blocks b
        #{if terms != [], do: "JOIN block_terms bt ON b.block_id = bt.block_id", else: ""}
        WHERE #{Enum.join(where_clauses, " AND ")}
        ORDER BY b.ts_min #{order_dir}
        """
      end

    {:ok, stmt} = Exqlite.Sqlite3.prepare(db, sql)

    if params != [] do
      Exqlite.Sqlite3.bind(stmt, params)
    end

    rows = collect_rows_3(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    rows
  end

  defp build_query_terms(term_filters) do
    Enum.flat_map(term_filters, fn
      {:name, name} -> ["name:#{name}"]
      {:kind, kind} -> ["kind:#{kind}"]
      {:status, status} -> ["status:#{status}"]
      {:service, svc} -> ["service.name:#{svc}"]
      {:attributes, map} -> Enum.map(map, fn {k, v} -> "#{k}:#{v}" end)
      _ -> []
    end)
  end

  defp build_where(terms, time_filters) do
    {term_clauses, term_params} =
      case terms do
        [] ->
          {[], []}

        terms ->
          placeholders = Enum.map_join(1..length(terms), ", ", &"?#{&1}")
          {["bt.term IN (#{placeholders})"], terms}
      end

    {time_clauses, time_params} =
      time_filters
      |> Enum.reduce({[], []}, fn
        {:since, ts}, {clauses, params} ->
          idx = length(term_params) + length(params) + 1
          {["b.ts_max >= ?#{idx}" | clauses], params ++ [to_nanos(ts)]}

        {:until, ts}, {clauses, params} ->
          idx = length(term_params) + length(params) + 1
          {["b.ts_min <= ?#{idx}" | clauses], params ++ [to_nanos(ts)]}
      end)

    {term_clauses ++ time_clauses, term_params ++ time_params}
  end

  defp to_nanos(%DateTime{} = dt), do: DateTime.to_unix(dt, :nanosecond)
  defp to_nanos(ts) when is_integer(ts), do: ts

  defp to_format_atom("raw"), do: :raw
  defp to_format_atom("zstd"), do: :zstd
  defp to_format_atom(:raw), do: :raw
  defp to_format_atom(:zstd), do: :zstd
  defp to_format_atom(_), do: :zstd

  # --- Service/operation discovery ---

  defp do_distinct_services(db) do
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT DISTINCT substr(term, 14) FROM block_terms
      WHERE term LIKE 'service.name:%'
      ORDER BY substr(term, 14)
      """)

    services = collect_single_column(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    {:ok, services}
  end

  defp do_distinct_operations(db, service) do
    # Find blocks that contain this service, then get span names from those blocks
    {:ok, stmt} =
      Exqlite.Sqlite3.prepare(db, """
      SELECT DISTINCT substr(bt2.term, 6) FROM block_terms bt1
      JOIN block_terms bt2 ON bt1.block_id = bt2.block_id
      WHERE bt1.term = ?1 AND bt2.term LIKE 'name:%'
      ORDER BY substr(bt2.term, 6)
      """)

    Exqlite.Sqlite3.bind(stmt, ["service.name:#{service}"])
    operations = collect_single_column(db, stmt)
    Exqlite.Sqlite3.release(db, stmt)
    {:ok, operations}
  end

  defp collect_single_column(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [value]} -> [value | collect_single_column(db, stmt)]
      :done -> []
    end
  end

  # --- Row collectors ---

  defp collect_rows_2(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path]} -> [{block_id, file_path} | collect_rows_2(db, stmt)]
      :done -> []
    end
  end

  defp collect_rows_3(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path, format]} ->
        [{block_id, file_path, format} | collect_rows_3(db, stmt)]

      :done ->
        []
    end
  end

  defp collect_rows_with_size(db, stmt) do
    case Exqlite.Sqlite3.step(db, stmt) do
      {:row, [block_id, file_path, size]} ->
        [{block_id, file_path, size} | collect_rows_with_size(db, stmt)]

      :done ->
        []
    end
  end
end
