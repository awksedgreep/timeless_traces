defmodule TimelessTraces.DiskTest do
  use ExUnit.Case, async: false

  @data_dir "test/tmp/disk_#{System.unique_integer([:positive])}"

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, @data_dir)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      File.rm_rf!(@data_dir)
      Application.put_env(:timeless_traces, :storage, :disk)
    end)

    :ok
  end

  defp make_span(overrides \\ %{}) do
    Map.merge(
      %{
        trace_id: "trace-#{System.unique_integer([:positive])}",
        span_id: "span-#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "HTTP GET /api",
        kind: :server,
        start_time: System.system_time(:nanosecond),
        end_time: System.system_time(:nanosecond) + 50_000_000,
        duration_ns: 50_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET", "service.name" => "api"},
        events: [],
        resource: %{"service.name" => "api"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  describe "disk storage" do
    test "creates blocks directory" do
      assert File.dir?(Path.join(@data_dir, "blocks"))
    end

    test "creates index files on startup" do
      # SQLite index DB is created when the DB GenServer starts
      assert File.exists?(Path.join(@data_dir, "traces_index.db"))
    end

    test "ingest, flush, and query with disk storage" do
      spans = [
        make_span(%{name: "GET /users", status: :ok}),
        make_span(%{name: "POST /users", status: :error}),
        make_span(%{name: "GET /health"})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: all, total: 3}} = TimelessTraces.query([])
      assert length(all) == 3
      assert Enum.all?(all, &match?(%TimelessTraces.Span{}, &1))
    end

    test "writes raw block files to disk" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) == 1

      data = File.read!(hd(raw_files))
      assert {:ok, entries} = TimelessTraces.Writer.decompress_block(data, :raw)
      assert length(entries) == 1
    end

    test "trace lookup works on disk" do
      trace_id = "disk-trace-#{System.unique_integer([:positive])}"

      spans = [
        make_span(%{trace_id: trace_id, span_id: "root", parent_span_id: nil, name: "root"}),
        make_span(%{trace_id: trace_id, span_id: "child", parent_span_id: "root", name: "child"})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, trace_spans} = TimelessTraces.trace(trace_id)
      assert length(trace_spans) == 2
    end

    test "stats reflect disk usage" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      {:ok, stats} = TimelessTraces.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.index_size > 0
      assert stats.raw_blocks == 1
    end

    test "multiple flushes create multiple block files" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      blocks_dir = Path.join(@data_dir, "blocks")
      raw_files = Path.wildcard(Path.join(blocks_dir, "*.raw"))
      assert length(raw_files) == 2

      {:ok, %TimelessTraces.Result{total: 2}} = TimelessTraces.query([])
    end

    test "filtering works on disk" do
      spans = [
        make_span(%{status: :ok}),
        make_span(%{status: :error})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: errors}} = TimelessTraces.query(status: :error)
      assert length(errors) == 1
      assert hd(errors).status == :error
    end

    test "full page walk is stable across multiple disk blocks" do
      spans =
        for idx <- 1..24 do
          make_span(%{
            name: "disk-span-#{idx}",
            start_time: idx * 100_000,
            end_time: idx * 100_000 + 1_000,
            attributes: %{"service.name" => "disk-pager", "host.name" => "trace-host"}
          })
        end

      spans
      |> Enum.chunk_every(4)
      |> Enum.each(fn chunk ->
        TimelessTraces.Buffer.ingest(chunk)
        TimelessTraces.flush()
      end)

      {:ok, %TimelessTraces.Result{entries: all_entries, total: 24}} =
        TimelessTraces.query(
          limit: 24,
          order: :desc,
          attributes: %{"service.name" => "disk-pager"}
        )

      expected_names = Enum.map(all_entries, & &1.name)
      page_size = 5

      paged_names =
        0..4
        |> Enum.flat_map(fn page_index ->
          offset = page_index * page_size

          {:ok,
           %TimelessTraces.Result{
             entries: entries,
             has_more: has_more,
             total: reported_total,
             offset: ^offset,
             limit: ^page_size
           }} =
            TimelessTraces.query(
              limit: page_size,
              offset: offset,
              order: :desc,
              count_total: false,
              attributes: %{"service.name" => "disk-pager"}
            )

          expected_count = min(page_size, max(length(expected_names) - offset, 0))

          assert length(entries) == expected_count

          assert Enum.map(entries, & &1.name) ==
                   Enum.slice(expected_names, offset, expected_count)

          assert has_more == offset + expected_count < length(expected_names)

          expected_total =
            if has_more do
              offset + expected_count + 1
            else
              length(expected_names)
            end

          assert reported_total == expected_total

          Enum.map(entries, & &1.name)
        end)

      assert paged_names == expected_names
      assert Enum.uniq(paged_names) == paged_names
    end

    test "count_total true and false return identical entries for the same disk page" do
      spans =
        for idx <- 1..12 do
          make_span(%{
            name: "disk-compare-#{idx}",
            start_time: idx * 1_000_000,
            end_time: idx * 1_000_000 + 100,
            attributes: %{"service.name" => "disk-compare", "host.name" => "trace-host"}
          })
        end

      spans
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        TimelessTraces.Buffer.ingest(chunk)
        TimelessTraces.flush()
      end)

      pagination = [
        limit: 4,
        offset: 4,
        order: :desc,
        attributes: %{"service.name" => "disk-compare"}
      ]

      {:ok, %TimelessTraces.Result{entries: exact_entries, total: exact_total, has_more: true}} =
        TimelessTraces.query(pagination)

      {:ok,
       %TimelessTraces.Result{
         entries: fast_entries,
         total: fast_total,
         has_more: fast_has_more
       }} =
        TimelessTraces.query(Keyword.put(pagination, :count_total, false))

      assert Enum.map(fast_entries, & &1.name) == Enum.map(exact_entries, & &1.name)
      assert fast_has_more
      assert exact_total == 12
      assert fast_total == 9
    end
  end
end
