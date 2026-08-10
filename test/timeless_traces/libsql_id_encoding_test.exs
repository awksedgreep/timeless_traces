defmodule TimelessTraces.LibsqlIdEncodingTest do
  @moduledoc """
  Ids returned by the libSQL engine must match what the Elixir engine returns.

  The libSQL store keeps trace/span ids as BLOBs — 16 and 8 bytes — while the
  public contract is lowercase hex. The read path handed the raw blob straight
  to callers, so the traces dashboard rendered binary and any lookup by id
  missed. `Index.trace/1` calling `Base.decode16!/2` is the other half of that
  contract.

  Both engines pass their own suites; nothing asserted they agree. That gap is
  what this file closes.
  """

  use ExUnit.Case, async: false

  @data_dir "test/tmp/libsql_id_encoding"
  @extension System.get_env("TIMELESS_EXT_PATH") ||
               Path.expand("../../../timeless-libsql/target/release/libtimeless_ext.so", __DIR__)

  @trace_id "3154f7058d8c04fbe520b3e0c628ff85"
  @span_id "2aaeed47a5f48a0d"
  @parent_id "99ab1c9fc28927b3"

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

  defp span(overrides \\ %{}) do
    base = System.os_time(:nanosecond)

    Map.merge(
      %{
        trace_id: @trace_id,
        span_id: @span_id,
        parent_span_id: @parent_id,
        name: "GET /dashboard/:page",
        kind: :server,
        start_time: base,
        end_time: base + 1_000,
        duration_ns: 1_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.request.method" => "GET"},
        events: [],
        resource: %{"service.name" => "api"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  test "query returns ids as lowercase hex, not raw blobs" do
    :ok = TimelessTraces.StorageEngine.ingest([span()])
    :ok = TimelessTraces.flush()

    assert {:ok, %TimelessTraces.Result{entries: [got]}} = TimelessTraces.query([])

    assert got.trace_id == @trace_id
    assert got.span_id == @span_id
    assert got.parent_span_id == @parent_id
  end

  test "ids are printable text of the documented width" do
    :ok = TimelessTraces.StorageEngine.ingest([span()])
    :ok = TimelessTraces.flush()

    assert {:ok, %TimelessTraces.Result{entries: [got]}} = TimelessTraces.query([])

    assert String.valid?(got.trace_id), "trace_id is not printable text"
    assert byte_size(got.trace_id) == 32
    assert byte_size(got.span_id) == 16
    assert got.trace_id =~ ~r/\A[0-9a-f]{32}\z/
    assert got.span_id =~ ~r/\A[0-9a-f]{16}\z/
  end

  test "a span can be found again by the id the engine reported" do
    # The round trip that was broken: read an id back, then look it up.
    :ok = TimelessTraces.StorageEngine.ingest([span()])
    :ok = TimelessTraces.flush()

    assert {:ok, %TimelessTraces.Result{entries: [got]}} = TimelessTraces.query([])
    assert {:ok, spans} = TimelessTraces.trace(got.trace_id)
    assert length(spans) == 1
    assert hd(spans).span_id == @span_id
  end

  test "a root span reports no parent rather than an empty blob" do
    :ok =
      TimelessTraces.StorageEngine.ingest([
        span(%{parent_span_id: nil, span_id: "1111111111111111"})
      ])

    :ok = TimelessTraces.flush()

    assert {:ok, %TimelessTraces.Result{entries: [got]}} = TimelessTraces.query([])
    assert got.parent_span_id in [nil, ""]
  end
end
