defmodule TimelessTracesTest do
  use ExUnit.Case, async: false

  setup do
    Application.stop(:timeless_traces)
    Application.put_env(:timeless_traces, :storage, :memory)
    Application.put_env(:timeless_traces, :data_dir, "test/tmp/e2e_should_not_exist")
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :retention_max_size, nil)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
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

  describe "full pipeline" do
    test "ingest → flush → query returns spans" do
      spans = [
        make_span(%{name: "GET /users", status: :ok}),
        make_span(%{name: "POST /users", status: :error}),
        make_span(%{name: "GET /health", status: :ok})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: all, total: 3}} = TimelessTraces.query([])
      assert length(all) == 3
      assert Enum.all?(all, &match?(%TimelessTraces.Span{}, &1))
    end

    test "query by status filter" do
      spans = [
        make_span(%{status: :ok}),
        make_span(%{status: :error}),
        make_span(%{status: :ok})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: errors, total: 1}} =
        TimelessTraces.query(status: :error)

      assert length(errors) == 1
      assert hd(errors).status == :error
    end

    test "query by kind filter" do
      spans = [
        make_span(%{kind: :server}),
        make_span(%{kind: :client}),
        make_span(%{kind: :internal})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: servers}} = TimelessTraces.query(kind: :server)
      assert length(servers) == 1
      assert hd(servers).kind == :server
    end

    test "query by host.name attribute filter" do
      spans = [
        make_span(%{
          name: "host-a",
          attributes: %{"http.method" => "GET", "service.name" => "api", "host.name" => "web-01"}
        }),
        make_span(%{
          name: "host-b",
          attributes: %{"http.method" => "GET", "service.name" => "api", "host.name" => "web-02"}
        })
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: results, total: 1}} =
        TimelessTraces.query(attributes: %{"host.name" => "web-01"})

      assert length(results) == 1
      assert hd(results).name == "host-a"
      assert hd(results).attributes["host.name"] == "web-01"
    end

    test "query with duration filters" do
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          start_time: now,
          end_time: now + 10_000_000,
          duration_ns: 10_000_000,
          name: "fast"
        }),
        make_span(%{
          start_time: now + 1,
          end_time: now + 1 + 200_000_000,
          duration_ns: 200_000_000,
          name: "slow"
        })
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: slow}} =
        TimelessTraces.query(min_duration: 100_000_000)

      assert length(slow) == 1
      assert hd(slow).name == "slow"
    end

    test "trace/1 retrieves full trace" do
      trace_id = "trace-full-#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root-span",
          start_time: now,
          end_time: now + 100_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child-1",
          parent_span_id: "root",
          name: "child-span-1",
          start_time: now + 10_000_000,
          end_time: now + 50_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child-2",
          parent_span_id: "root",
          name: "child-span-2",
          start_time: now + 20_000_000,
          end_time: now + 80_000_000
        }),
        make_span(%{name: "unrelated"})
      ]

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, trace_spans} = TimelessTraces.trace(trace_id)
      assert length(trace_spans) == 3
      assert Enum.all?(trace_spans, &(&1.trace_id == trace_id))

      # Should be sorted by start_time
      times = Enum.map(trace_spans, & &1.start_time)
      assert times == Enum.sort(times)
    end

    test "pagination works" do
      spans = for i <- 1..20, do: make_span(%{name: "span-#{i}"})
      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: page1, total: 20, limit: 5}} =
        TimelessTraces.query(limit: 5)

      assert length(page1) == 5

      {:ok, %TimelessTraces.Result{entries: page2, offset: 5}} =
        TimelessTraces.query(limit: 5, offset: 5)

      assert length(page2) == 5
      assert hd(page1).span_id != hd(page2).span_id
    end

    test "pagination can skip exact totals" do
      spans = for i <- 1..20, do: make_span(%{name: "span-fast-#{i}"})
      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{entries: page1, has_more: has_more, limit: 5}} =
        TimelessTraces.query(limit: 5, count_total: false)

      assert length(page1) == 5
      assert has_more

      {:ok, %TimelessTraces.Result{entries: page4, has_more: has_more_last, offset: 15}} =
        TimelessTraces.query(limit: 5, offset: 15, count_total: false)

      assert length(page4) == 5
      refute has_more_last
    end

    test "stats/0 returns aggregate data" do
      {:ok, stats} = TimelessTraces.stats()
      assert stats.total_blocks == 0

      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      {:ok, stats} = TimelessTraces.stats()
      assert stats.total_blocks == 1
      assert stats.total_entries == 1
      assert stats.total_bytes > 0
    end

    test "subscribe/unsubscribe for live spans" do
      TimelessTraces.subscribe()

      span = make_span(%{name: "live-span"})
      TimelessTraces.Buffer.ingest([span])

      assert_receive {:timeless_traces, :span, %TimelessTraces.Span{name: "live-span"}}, 1000

      TimelessTraces.unsubscribe()
    end

    test "multiple flushes accumulate" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      {:ok, %TimelessTraces.Result{total: 3}} = TimelessTraces.query([])
    end

    test "no block files created in memory mode" do
      TimelessTraces.Buffer.ingest([make_span()])
      TimelessTraces.flush()

      # SQLite index file may exist, but no block files should be written
      refute File.exists?("test/tmp/e2e_should_not_exist/blocks")
    end

    test "count_total false preserves page slices for multi-block memory queries" do
      service_name = "pager-#{System.unique_integer([:positive])}"

      spans =
        for idx <- 1..18 do
          make_span(%{
            name: "memory-span-#{idx}",
            start_time: idx * 1_000,
            end_time: idx * 1_000 + 100,
            attributes: %{"service.name" => service_name, "host.name" => "memory"}
          })
        end

      spans
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        TimelessTraces.Buffer.ingest(chunk)
        TimelessTraces.flush()
      end)

      {:ok, %TimelessTraces.Result{entries: all_entries, total: 18}} =
        TimelessTraces.query(
          limit: 18,
          order: :desc,
          attributes: %{"service.name" => service_name}
        )

      expected_names = Enum.map(all_entries, & &1.name)
      page_size = 4

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
              attributes: %{"service.name" => service_name}
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

    test "count_total true and false return identical entries for the same trace page" do
      service_name = "compare-#{System.unique_integer([:positive])}"

      spans =
        for idx <- 1..12 do
          make_span(%{
            name: "compare-span-#{idx}",
            start_time: idx * 10_000,
            end_time: idx * 10_000 + 500,
            attributes: %{"service.name" => service_name, "host.name" => "memory"}
          })
        end

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      pagination = [
        limit: 4,
        offset: 4,
        order: :desc,
        attributes: %{"service.name" => service_name}
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

    test "filter combinations preserve exact page slices for count_total false" do
      service_name = "combo-#{System.unique_integer([:positive])}"

      spans =
        for idx <- 1..18 do
          status = if rem(idx, 2) == 0, do: :error, else: :ok
          kind = if rem(idx, 3) == 0, do: :client, else: :server

          make_span(%{
            name: "combo-span-#{idx}",
            kind: kind,
            status: status,
            start_time: idx * 100_000,
            end_time: idx * 100_000 + idx * 1_000,
            duration_ns: idx * 1_000,
            attributes: %{"service.name" => service_name, "host.name" => "combo-host"}
          })
        end

      spans
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        TimelessTraces.Buffer.ingest(chunk)
        TimelessTraces.flush()
      end)

      filters = [
        attributes: %{"service.name" => service_name},
        status: :error,
        kind: :server,
        min_duration: 2_000,
        max_duration: 16_000,
        order: :desc
      ]

      {:ok, %TimelessTraces.Result{entries: all_entries, total: total}} =
        TimelessTraces.query([limit: 20] ++ filters)

      expected_names = Enum.map(all_entries, & &1.name)
      assert total > 0

      for offset <- [0, 2] do
        {:ok, %TimelessTraces.Result{entries: exact_entries, total: ^total}} =
          TimelessTraces.query([limit: 2, offset: offset] ++ filters)

        {:ok,
         %TimelessTraces.Result{
           entries: fast_entries,
           total: fast_total,
           has_more: fast_has_more
         }} =
          TimelessTraces.query([limit: 2, offset: offset, count_total: false] ++ filters)

        expected_count = min(2, max(length(expected_names) - offset, 0))

        assert Enum.map(exact_entries, & &1.name) ==
                 Enum.slice(expected_names, offset, expected_count)

        assert Enum.map(fast_entries, & &1.name) == Enum.map(exact_entries, & &1.name)
        assert fast_has_more == offset + expected_count < total
        assert fast_total == if(fast_has_more, do: offset + expected_count + 1, else: total)
      end
    end

    test "equal start times still produce stable page equivalence" do
      service_name = "same-start-#{System.unique_integer([:positive])}"
      start_time = 123_456_789

      spans =
        for idx <- 1..12 do
          make_span(%{
            name: "same-start-#{idx}",
            start_time: start_time,
            end_time: start_time + idx,
            duration_ns: idx,
            attributes: %{"service.name" => service_name, "host.name" => "same-host"}
          })
        end

      TimelessTraces.Buffer.ingest(spans)
      TimelessTraces.flush()

      pagination = [
        limit: 4,
        offset: 4,
        order: :desc,
        attributes: %{"service.name" => service_name}
      ]

      {:ok, %TimelessTraces.Result{entries: exact_entries, total: 12}} =
        TimelessTraces.query(pagination)

      {:ok, %TimelessTraces.Result{entries: fast_entries, total: fast_total, has_more: true}} =
        TimelessTraces.query(Keyword.put(pagination, :count_total, false))

      assert Enum.map(fast_entries, & &1.name) == Enum.map(exact_entries, & &1.name)
      assert fast_total == 9
    end

    test "time filters preserve exact page slices for count_total false" do
      service_name = "time-#{System.unique_integer([:positive])}"
      base = 10_000_000

      spans =
        for idx <- 1..18 do
          make_span(%{
            name: "time-span-#{idx}",
            start_time: base + idx * 1_000,
            end_time: base + idx * 1_000 + 100,
            duration_ns: 100,
            attributes: %{"service.name" => service_name, "host.name" => "time-host"}
          })
        end

      spans
      |> Enum.chunk_every(3)
      |> Enum.each(fn chunk ->
        TimelessTraces.Buffer.ingest(chunk)
        TimelessTraces.flush()
      end)

      filters = [
        attributes: %{"service.name" => service_name},
        since: base + 5_000,
        until: base + 14_000,
        order: :asc
      ]

      {:ok, %TimelessTraces.Result{entries: all_entries, total: 10}} =
        TimelessTraces.query([limit: 10] ++ filters)

      expected_names = Enum.map(all_entries, & &1.name)

      for offset <- [0, 4, 8] do
        {:ok, %TimelessTraces.Result{entries: exact_entries, total: 10}} =
          TimelessTraces.query([limit: 4, offset: offset] ++ filters)

        {:ok,
         %TimelessTraces.Result{
           entries: fast_entries,
           total: fast_total,
           has_more: fast_has_more
         }} =
          TimelessTraces.query([limit: 4, offset: offset, count_total: false] ++ filters)

        expected_count = min(4, max(length(expected_names) - offset, 0))

        assert Enum.map(exact_entries, & &1.name) ==
                 Enum.slice(expected_names, offset, expected_count)

        assert Enum.map(fast_entries, & &1.name) == Enum.map(exact_entries, & &1.name)
        assert fast_has_more == offset + expected_count < 10
        assert fast_total == if(fast_has_more, do: offset + expected_count + 1, else: 10)
      end
    end
  end
end
