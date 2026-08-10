defmodule TimelessTraces.LibsqlFacadeTest do
  # The public facade running on engine: :libsql — the port's
  # engine-contract test. Serial: restarts the OTP app with swapped env.
  use ExUnit.Case, async: false

  @data_dir "test/tmp/libsql_facade"
  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)

    previous = %{
      engine: Application.get_env(:timeless_traces, :engine),
      data_dir: Application.get_env(:timeless_traces, :data_dir),
      extension_path: Application.get_env(:timeless_traces, :extension_path)
    }

    Application.put_env(:timeless_traces, :engine, :libsql)
    Application.put_env(:timeless_traces, :data_dir, @data_dir)
    Application.put_env(:timeless_traces, :extension_path, @extension)
    {:ok, _} = Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)

      for {key, value} <- previous do
        case value do
          nil -> Application.delete_env(:timeless_traces, key)
          _ -> Application.put_env(:timeless_traces, key, value)
        end
      end

      File.rm_rf!(@data_dir)
      {:ok, _} = Application.ensure_all_started(:timeless_traces)
    end)

    :ok
  end

  defp span(i, status) do
    start = 1_700_000_000_000_000_000 + i * 1_000_000

    %{
      trace_id: <<i::128>>,
      span_id: <<i::64>>,
      parent_span_id: nil,
      name: "GET /users",
      kind: :server,
      start_time: start,
      end_time: start + 250_000,
      duration_ns: 250_000,
      status: status,
      status_message: nil,
      attributes: %{"service.name" => "api"},
      events: [],
      resource: %{},
      instrumentation_scope: nil
    }
  end

  test "the public facade works end to end on the libSQL engine" do
    assert {:ok, _} = TimelessTraces.subscribe(status: :error)

    spans = [span(1, :ok), span(2, :error), span(3, :ok)]
    assert :ok = TimelessTraces.StorageEngine.ingest(spans)

    assert_receive {:timeless_traces, :span, %TimelessTraces.Span{status: :error}}
    refute_receive {:timeless_traces, :span, %TimelessTraces.Span{status: :ok}}, 50
    :ok = TimelessTraces.unsubscribe()

    assert :ok = TimelessTraces.flush()

    assert {:ok, %TimelessTraces.Result{total: 3}} = TimelessTraces.query([])
    assert {:ok, %TimelessTraces.Result{total: 1}} = TimelessTraces.query(status: :error)
    assert {:ok, %TimelessTraces.Result{total: 3}} = TimelessTraces.query(service: "api")

    # Ids come back as lowercase hex regardless of whether they went in as a
    # blob or as hex text; the store keeps blobs, the public contract is hex.
    span_2_hex = Base.encode16(<<2::64>>, case: :lower)

    assert {:ok, [%TimelessTraces.Span{span_id: ^span_2_hex}]} =
             TimelessTraces.trace(<<2::128>>)

    assert {:ok, ["api"]} = TimelessTraces.services()
    assert {:ok, ["GET /users"]} = TimelessTraces.operations("api")

    assert {:ok, %TimelessTraces.Stats{total_entries: 3}} = TimelessTraces.stats()
    assert :ok = TimelessTraces.merge_now()

    backup_dir = Path.join(@data_dir, "backup")
    assert {:ok, %{files: ["traces.db"], total_bytes: bytes}} = TimelessTraces.backup(backup_dir)
    assert bytes > 0
  end
end
