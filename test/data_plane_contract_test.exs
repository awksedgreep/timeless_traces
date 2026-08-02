defmodule TimelessTraces.DataPlaneContractTest do
  use ExUnit.Case, async: false

  @port 31_029
  @data_dir "test/tmp/data_plane_contract"
  @trace_id "00112233445566778899aabbccddeeff"
  @root_id "0102030405060708"
  @child_id "1112131415161718"

  setup do
    Application.stop(:timeless_traces)
    File.rm_rf!(@data_dir)
    Application.put_env(:timeless_traces, :storage, :disk)
    Application.put_env(:timeless_traces, :data_dir, @data_dir)
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :compaction_threshold, 1_000_000)
    Application.put_env(:timeless_traces, :retention_max_age, nil)
    Application.put_env(:timeless_traces, :http, false)
    Application.ensure_all_started(:timeless_traces)

    start_supervised!({TimelessTraces.HTTP, port: @port})
    :persistent_term.put({TimelessTraces.HTTP, :bearer_token}, nil)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      File.rm_rf!(@data_dir)
    end)

    :ok
  end

  test "rich OTLP JSON survives HTTP, flush, Jaeger rendering, and cold reopen" do
    response = post(json_fixture(), "application/json")
    assert response.status == 200
    assert :json.decode(response.body) == %{"partialSuccess" => %{}}

    assert_drained_contract()
    assert_storage_contract()
    assert_jaeger_contract()

    Application.stop(:timeless_traces)
    Application.ensure_all_started(:timeless_traces)

    assert_storage_contract()
  end

  test "rich OTLP protobuf has the same storage contract" do
    response = post(protobuf_fixture(), "application/x-protobuf")
    assert response.status == 200
    assert :json.decode(response.body) == %{"partialSuccess" => %{}}

    assert_drained_contract()
    assert_storage_contract()
  end

  test "gzip protobuf has the same storage contract and counts compressed request bytes" do
    body = :zlib.gzip(protobuf_fixture())

    response =
      TimelessTraces.TestHTTP.post(
        @port,
        "/insert/opentelemetry/v1/traces",
        body,
        content_type: "application/x-protobuf",
        headers: [{"content-encoding", "gzip"}]
      )

    assert response.status == 200
    data_plane = flush_body()["data_plane"]
    assert data_plane["admitted_bytes"] == byte_size(body)
    assert data_plane["completed_spans"] == 2
    assert_storage_contract()
  end

  test "health separates admission, durable completion, rejection, and drain" do
    invalid = post("not json", "application/json")
    assert invalid.status == 400

    accepted = post(json_fixture(), "application/json")
    assert accepted.status == 200

    before = get_json("/health")["data_plane"]
    assert before["admitted_requests"] == 1
    assert before["admitted_spans"] == 2
    assert before["rejected_requests"] == 1
    assert before["drained_requests"] == 0

    after_flush = flush_body()["data_plane"]
    assert after_flush["drained_requests"] == 1
    assert after_flush["completed_spans"] == 2
    assert after_flush["failed_spans"] == 0
    assert after_flush["queued_spans"] == 0
    assert after_flush["in_flight_batches"] == 0
    assert after_flush["in_flight_spans"] == 0
    assert after_flush["oldest_queue_age_ms"] == 0
  end

  test "flush is a barrier for casts sent by independent producers" do
    spans =
      for producer <- 1..32,
          offset <- 1..16 do
        %{
          trace_id: String.pad_leading(Integer.to_string(producer, 16), 32, "0"),
          span_id: String.pad_leading(Integer.to_string(offset, 16), 16, "0"),
          parent_span_id: nil,
          name: "barrier",
          kind: :internal,
          start_time: 1_700_000_000_000_000_000 + producer * 1_000 + offset,
          end_time: 1_700_000_000_000_001_000 + producer * 1_000 + offset,
          duration_ns: 1_000,
          status: :unset,
          status_message: nil,
          attributes: %{"service.name" => "barrier"},
          events: [],
          resource: %{"service.name" => "barrier"},
          instrumentation_scope: nil
        }
      end

    spans
    |> Enum.chunk_every(16)
    |> Task.async_stream(&TimelessTraces.Buffer.ingest/1, max_concurrency: 32)
    |> Stream.run()

    TimelessTraces.flush()

    stats = TimelessTraces.DataPlaneStats.snapshot()
    assert stats.admitted_spans == 512
    assert stats.completed_spans == 512
    assert stats.failed_spans == 0
    assert stats.queued_spans == 0
    assert stats.in_flight_spans == 0
    assert TimelessTraces.stats() |> elem(1) |> Map.fetch!(:total_entries) == 512
  end

  defp assert_drained_contract do
    data_plane = flush_body()["data_plane"]
    assert data_plane["admitted_requests"] == 1
    assert data_plane["admitted_spans"] == 2
    assert data_plane["drained_requests"] == 1
    assert data_plane["completed_spans"] == 2
    assert data_plane["rejected_requests"] == 0
    assert data_plane["failed_spans"] == 0
    assert data_plane["queued_spans"] == 0
    assert data_plane["in_flight_batches"] == 0
    assert data_plane["in_flight_spans"] == 0
  end

  defp assert_storage_contract do
    {:ok, spans} = TimelessTraces.trace(@trace_id)
    assert length(spans) == 2

    root = Enum.find(spans, &(&1.span_id == @root_id))
    child = Enum.find(spans, &(&1.span_id == @child_id))

    assert root.trace_id == @trace_id
    assert root.parent_span_id == nil
    assert root.name == "GET /contract"
    assert root.kind == :server
    assert root.start_time == 1_700_000_000_000_000_000
    assert root.end_time == 1_700_000_000_120_000_000
    assert root.duration_ns == 120_000_000
    assert root.status == :error
    assert root.status_message == "contract failure"

    assert root.attributes == %{
             "http.method" => "GET",
             "http.status_code" => 503,
             "retryable" => true,
             "score" => 0.75
           }

    assert root.events == [
             %{
               name: "exception",
               timestamp: 1_700_000_000_040_000_000,
               attributes: %{"exception.type" => "ContractError", "handled" => false}
             }
           ]

    assert root.resource == %{
             "debug" => false,
             "replica" => 7,
             "service.name" => "contract-svc",
             "service.version" => "1.2.3"
           }

    assert root.instrumentation_scope == %{name: "contract-lib", version: "4.5.6"}

    assert child.parent_span_id == @root_id
    assert child.name == "DB contract"
    assert child.kind == :client
    assert child.duration_ns == 60_000_000
    assert child.status == :unset
    assert child.status_message == nil
    assert child.attributes == %{"db.system" => "libsql", "rows" => 3}
    assert child.events == []
    assert child.resource == root.resource
    assert child.instrumentation_scope == root.instrumentation_scope
  end

  defp assert_jaeger_contract do
    services = get_json("/select/jaeger/api/services")

    assert services == %{
             "data" => ["contract-svc"],
             "errors" => :null,
             "limit" => 0,
             "offset" => 0,
             "total" => 1
           }

    operations = get_json("/select/jaeger/api/services/contract-svc/operations")
    assert operations["data"] == ["DB contract", "GET /contract"]

    detail = get_json("/select/jaeger/api/traces/#{@trace_id}")
    assert detail["errors"] == :null
    assert detail["total"] == 1
    [trace] = detail["data"]
    assert trace["traceID"] == @trace_id
    assert map_size(trace["processes"]) == 1

    [process] = Map.values(trace["processes"])
    assert process["serviceName"] == "contract-svc"

    assert tag_map(process["tags"]) == %{
             "debug" => {"bool", false},
             "replica" => {"int64", 7},
             "service.version" => {"string", "1.2.3"}
           }

    spans = Map.new(trace["spans"], &{&1["spanID"], &1})
    root = spans[@root_id]
    child = spans[@child_id]

    assert root["operationName"] == "GET /contract"
    assert root["startTime"] == 1_700_000_000_000_000
    assert root["duration"] == 120_000
    assert root["references"] == []

    assert tag_map(root["tags"]) == %{
             "http.method" => {"string", "GET"},
             "http.status_code" => {"int64", 503},
             "otel.status_code" => {"string", "ERROR"},
             "otel.status_description" => {"string", "contract failure"},
             "retryable" => {"bool", true},
             "score" => {"float64", 0.75},
             "span.kind" => {"string", "server"}
           }

    assert root["logs"] == [
             %{
               "timestamp" => 1_700_000_000_040_000,
               "fields" => [
                 %{"key" => "event", "type" => "string", "value" => "exception"},
                 %{"key" => "exception.type", "type" => "string", "value" => "ContractError"},
                 %{"key" => "handled", "type" => "bool", "value" => false}
               ]
             }
           ]

    assert child["references"] == [
             %{"refType" => "CHILD_OF", "traceID" => @trace_id, "spanID" => @root_id}
           ]

    # The established search implementation filters and limits spans before
    # grouping. Pin that behavior explicitly so Session 4 cannot accidentally
    # benchmark a semantically easier trace search.
    search =
      get_json(
        "/select/jaeger/api/traces?service=contract-svc&operation=GET%20%2Fcontract&limit=10"
      )

    [search_trace] = search["data"]
    assert Enum.map(search_trace["spans"], & &1["spanID"]) == [@root_id]
  end

  defp tag_map(tags) do
    Map.new(tags, fn tag -> {tag["key"], {tag["type"], tag["value"]}} end)
  end

  defp post(body, content_type) do
    TimelessTraces.TestHTTP.post(
      @port,
      "/insert/opentelemetry/v1/traces",
      body,
      content_type: content_type
    )
  end

  defp flush_body, do: get_json("/api/v1/flush")

  defp get_json(path) do
    response = TimelessTraces.TestHTTP.get(@port, path)
    assert response.status == 200
    :json.decode(response.body)
  end

  defp json_fixture do
    File.read!("test/fixtures/data_plane/rich_trace.otlp.json")
  end

  defp protobuf_fixture do
    :opentelemetry_exporter_trace_service_pb.encode_msg(
      %{
        resource_spans: [
          %{
            resource: %{
              attributes: [
                pb_attr("service.name", {:string_value, "contract-svc"}),
                pb_attr("service.version", {:string_value, "1.2.3"}),
                pb_attr("replica", {:int_value, 7}),
                pb_attr("debug", {:bool_value, false})
              ]
            },
            scope_spans: [
              %{
                scope: %{name: "contract-lib", version: "4.5.6"},
                spans: [
                  %{
                    trace_id: Base.decode16!(@trace_id, case: :mixed),
                    span_id: Base.decode16!(@root_id, case: :mixed),
                    parent_span_id: <<>>,
                    name: "GET /contract",
                    kind: :SPAN_KIND_SERVER,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_120_000_000,
                    status: %{code: :STATUS_CODE_ERROR, message: "contract failure"},
                    attributes: [
                      pb_attr("http.method", {:string_value, "GET"}),
                      pb_attr("http.status_code", {:int_value, 503}),
                      pb_attr("retryable", {:bool_value, true}),
                      pb_attr("score", {:double_value, 0.75})
                    ],
                    events: [
                      %{
                        name: "exception",
                        time_unix_nano: 1_700_000_000_040_000_000,
                        attributes: [
                          pb_attr("exception.type", {:string_value, "ContractError"}),
                          pb_attr("handled", {:bool_value, false})
                        ]
                      }
                    ],
                    links: []
                  },
                  %{
                    trace_id: Base.decode16!(@trace_id, case: :mixed),
                    span_id: Base.decode16!(@child_id, case: :mixed),
                    parent_span_id: Base.decode16!(@root_id, case: :mixed),
                    name: "DB contract",
                    kind: :SPAN_KIND_CLIENT,
                    start_time_unix_nano: 1_700_000_000_020_000_000,
                    end_time_unix_nano: 1_700_000_000_080_000_000,
                    status: %{code: :STATUS_CODE_UNSET, message: ""},
                    attributes: [
                      pb_attr("db.system", {:string_value, "libsql"}),
                      pb_attr("rows", {:int_value, 3})
                    ],
                    events: [],
                    links: []
                  }
                ]
              }
            ]
          }
        ]
      },
      :export_trace_service_request
    )
  end

  defp pb_attr(key, value), do: %{key: key, value: %{value: value}}
end
