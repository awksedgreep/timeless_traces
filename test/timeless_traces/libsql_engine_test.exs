defmodule TimelessTraces.LibsqlEngineTest do
  use ExUnit.Case, async: false

  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  setup do
    dir = Path.join(System.tmp_dir!(), "tt_libsql_engine_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp start_engine!(dir) do
    start_supervised!({TimelessTraces.LibsqlEngine, data_dir: dir, extension_path: @extension})
  end

  defp span(i, opts \\ []) do
    start = 1_700_000_000_000_000_000 + i * 1_000_000
    duration = Keyword.get(opts, :duration_ns, 500_000)

    %{
      trace_id: <<Keyword.get(opts, :trace, i)::128>>,
      span_id: <<i::64>>,
      parent_span_id: nil,
      name: Keyword.get(opts, :name, "op-#{rem(i, 3)}"),
      kind: Keyword.get(opts, :kind, :server),
      start_time: start,
      end_time: start + duration,
      duration_ns: duration,
      status: Keyword.get(opts, :status, :ok),
      status_message: nil,
      attributes: %{"service.name" => Keyword.get(opts, :service, "svc-#{rem(i, 2)}")},
      events: [],
      resource: %{},
      instrumentation_scope: nil
    }
  end

  test "write, query, trace, discovery, stats, backup round-trip", %{dir: dir} do
    start_engine!(dir)

    {readers, _cursor} =
      :persistent_term.get({TimelessTraces.LibsqlEngine, :reader_pool})

    assert tuple_size(readers) == TimelessTraces.Config.libsql_reader_pool_size()
    assert readers |> Tuple.to_list() |> Enum.all?(&Process.alive?/1)

    assert {:ok, [[-128_000]]} = TimelessTraces.LibsqlEngine.sql("PRAGMA cache_size")
    assert {:ok, [[1_000]]} = TimelessTraces.LibsqlEngine.sql("PRAGMA wal_autocheckpoint")
    assert {:ok, [[2]]} = TimelessTraces.LibsqlEngine.sql("PRAGMA temp_store")

    spans = for i <- 1..10, do: span(i, status: if(rem(i, 5) == 0, do: :error, else: :ok))
    assert :ok = TimelessTraces.LibsqlEngine.ingest(spans)
    assert :ok = TimelessTraces.LibsqlEngine.flush()

    # Query: default newest-first, Span structs, exact totals.
    assert {:ok, %TimelessTraces.Result{entries: [newest | _], total: 10}} =
             TimelessTraces.LibsqlEngine.query([])

    # The engine reports ids as lowercase hex, matching the Elixir engine.
    span_10_hex = Base.encode16(<<10::64>>, case: :lower)
    assert %TimelessTraces.Span{span_id: ^span_10_hex} = newest

    # Status pushdown + residual parity via the shared Filter.
    assert {:ok, %TimelessTraces.Result{total: 2}} =
             TimelessTraces.LibsqlEngine.query(status: :error)

    assert {:ok,
            %TimelessTraces.Result{
              entries: paged,
              total: 8,
              limit: 2,
              offset: 1,
              has_more: true
            }} = TimelessTraces.LibsqlEngine.query(status: :ok, limit: 2, offset: 1)

    assert length(paged) == 2

    assert {:ok, %TimelessTraces.Result{total: 4, has_more: true}} =
             TimelessTraces.LibsqlEngine.query(
               status: :ok,
               limit: 2,
               offset: 1,
               count_total: false
             )

    # Service filter matches service.name attribute.
    assert {:ok, %TimelessTraces.Result{total: 5}} =
             TimelessTraces.LibsqlEngine.query(service: "svc-0")

    # Duration range.
    assert {:ok, %TimelessTraces.Result{total: 10}} =
             TimelessTraces.LibsqlEngine.query(min_duration: 400_000)

    assert {:ok, %TimelessTraces.Result{total: 0}} =
             TimelessTraces.LibsqlEngine.query(min_duration: 600_000)

    # trace/1 accepts raw ids and returns ascending spans.
    span_7_hex = Base.encode16(<<7::64>>, case: :lower)

    assert {:ok, [%TimelessTraces.Span{span_id: ^span_7_hex}]} =
             TimelessTraces.LibsqlEngine.trace(<<7::128>>)

    # ...and 32-char hex ids.
    hex = Base.encode16(<<7::128>>, case: :lower)

    assert {:ok, [%TimelessTraces.Span{span_id: ^span_7_hex}]} =
             TimelessTraces.LibsqlEngine.trace(hex)

    # Discovery.
    assert {:ok, ["svc-0", "svc-1"]} = TimelessTraces.LibsqlEngine.services()
    assert {:ok, ops} = TimelessTraces.LibsqlEngine.operations("svc-0")
    assert "op-0" in ops or "op-1" in ops or "op-2" in ops

    # Stats + backup.
    assert {:ok, %TimelessTraces.Stats{total_entries: 10}} = TimelessTraces.LibsqlEngine.stats()

    backup_dir = Path.join(dir, "backup")

    assert {:ok, %{files: ["traces.db"], total_bytes: bytes}} =
             TimelessTraces.LibsqlEngine.backup(backup_dir)

    assert bytes > 0
  end

  test "stats reports compressed blocks after optimize (dashboard tile contract)", %{dir: dir} do
    start_engine!(dir)

    assert :ok = TimelessTraces.LibsqlEngine.ingest(Enum.map(1..50, &span/1))
    assert :ok = TimelessTraces.LibsqlEngine.flush()

    assert {:ok, %TimelessTraces.Stats{} = raw_stats} = TimelessTraces.LibsqlEngine.stats()
    assert raw_stats.raw_blocks > 0
    assert raw_stats.compressed_blocks == 0

    assert {:ok, _} = TimelessTraces.LibsqlEngine.optimize()

    assert {:ok, %TimelessTraces.Stats{} = stats} = TimelessTraces.LibsqlEngine.stats()
    # compressed_*, not zstd_*: the libSQL engine writes adaptive columnar
    # blocks; the per-format zstd/openzl fields belong to the legacy engine.
    assert stats.raw_blocks == 0
    assert stats.compressed_blocks > 0
    assert stats.compressed_bytes > 0
    assert stats.zstd_blocks == 0
    assert stats.storage_mode == :libsql
    # Extension 0.8.x deliberately reports NULL here: exact dbstat accounting
    # would turn every routine stats read into a full index walk. The facade
    # preserves its integer compatibility field as zero.
    assert stats.index_size == 0
    assert stats.compaction_count > 0
    assert stats.total_blocks == stats.compressed_blocks
    assert stats.compression_raw_bytes_in > 0
    assert stats.compression_compressed_bytes_out > 0

    # Persisted in the store, not the process — must survive a restart.
    stop_supervised!(TimelessTraces.LibsqlEngine)
    start_engine!(dir)
    assert {:ok, %TimelessTraces.Stats{} = reopened} = TimelessTraces.LibsqlEngine.stats()
    assert reopened.compression_raw_bytes_in == stats.compression_raw_bytes_in
    assert reopened.compression_compressed_bytes_out == stats.compression_compressed_bytes_out
  end

  test "cold reopen preserves spans ingested without an explicit flush", %{dir: dir} do
    start_engine!(dir)
    assert :ok = TimelessTraces.LibsqlEngine.ingest(for i <- 1..5, do: span(i))
    :ok = stop_supervised!(TimelessTraces.LibsqlEngine)

    start_engine!(dir)
    assert {:ok, %TimelessTraces.Result{total: 5}} = TimelessTraces.LibsqlEngine.query([])
  end

  test "refuses an unmigrated legacy block store when auto_migrate is off", %{dir: dir} do
    File.mkdir_p!(dir)
    File.touch!(Path.join(dir, "traces_index.db"))

    assert {:error, _} =
             start_supervised(
               {TimelessTraces.LibsqlEngine,
                data_dir: dir, extension_path: @extension, auto_migrate: false}
             )
  end

  test "auto-converts a legacy block store at startup", %{dir: dir} do
    # Build a real legacy store through the running app.
    Application.stop(:timeless_traces)

    previous = Application.get_env(:timeless_traces, :data_dir)
    Application.put_env(:timeless_traces, :data_dir, dir)
    {:ok, _} = Application.ensure_all_started(:timeless_traces)

    :ok = TimelessTraces.StorageEngine.ingest([span(1), span(2, status: :error)])
    :ok = TimelessTraces.flush()
    Application.stop(:timeless_traces)

    case previous do
      nil -> Application.delete_env(:timeless_traces, :data_dir)
      _ -> Application.put_env(:timeless_traces, :data_dir, previous)
    end

    on_exit(fn -> {:ok, _} = Application.ensure_all_started(:timeless_traces) end)

    assert File.exists?(Path.join(dir, "traces_index.db"))

    # Default startup on :libsql converts automatically, then serves it.
    start_engine!(dir)

    assert {:ok, %TimelessTraces.Result{total: 2}} = TimelessTraces.LibsqlEngine.query([])

    assert {:ok, %TimelessTraces.Result{total: 1}} =
             TimelessTraces.LibsqlEngine.query(status: :error)

    # The source is retained for rollback.
    assert File.exists?(Path.join(dir, "traces_index.db"))
  end
end
