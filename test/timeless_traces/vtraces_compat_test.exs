defmodule TimelessTraces.VTracesCompatTest do
  @moduledoc """
  Compatibility tests that verify TimelessTraces HTTP responses match
  VictoriaTraces Jaeger API format. These tests compare our response structure
  against the real VictoriaTraces Jaeger API contract to catch wire-format
  mismatches that would break Jaeger UI, Grafana, or other consumers.

  Verified against VictoriaTraces at 192.168.160.225:10428.
  """
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  setup do
    Application.stop(:timeless_traces)
    Application.put_env(:timeless_traces, :storage, :memory)
    Application.put_env(:timeless_traces, :data_dir, "test/tmp/vtraces_compat")
    Application.put_env(:timeless_traces, :flush_interval, 60_000)
    Application.put_env(:timeless_traces, :max_buffer_size, 10_000)
    Application.put_env(:timeless_traces, :http, false)
    Application.ensure_all_started(:timeless_traces)

    on_exit(fn ->
      Application.stop(:timeless_traces)
      Application.put_env(:timeless_traces, :storage, :disk)
    end)

    :ok
  end

  defp call(conn) do
    TimelessTraces.HTTP.call(conn, TimelessTraces.HTTP.init([]))
  end

  defp json_body(conn) do
    :json.decode(conn.resp_body)
  end

  defp ingest_realistic_spans do
    now = System.system_time(:nanosecond)
    trace_id_1 = "aaaa111122223333444455556666777" <> "8"
    trace_id_2 = "bbbb111122223333444455556666777" <> "8"

    spans = [
      # Trace 1: HTTP request with DB query child
      %{
        trace_id: trace_id_1,
        span_id: "1111222233334444",
        parent_span_id: nil,
        name: "GET /api/v1/users",
        kind: :server,
        start_time: now - 500_000_000,
        end_time: now - 400_000_000,
        duration_ns: 100_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{
          "http.method" => "GET",
          "http.target" => "/api/v1/users",
          "http.status_code" => 200,
          "service.name" => "web-api"
        },
        events: [
          %{
            name: "request.started",
            timestamp: now - 500_000_000,
            attributes: %{"client_ip" => "10.0.0.1"}
          }
        ],
        resource: %{
          "service.name" => "web-api",
          "deployment.environment" => "test",
          "telemetry.sdk.language" => "erlang"
        },
        instrumentation_scope: %{name: "otel-test", version: "1.0"}
      },
      %{
        trace_id: trace_id_1,
        span_id: "5555666677778888",
        parent_span_id: "1111222233334444",
        name: "SELECT users",
        kind: :client,
        start_time: now - 490_000_000,
        end_time: now - 420_000_000,
        duration_ns: 70_000_000,
        status: :unset,
        status_message: nil,
        attributes: %{
          "db.system" => "postgresql",
          "db.statement" => "SELECT * FROM users",
          "service.name" => "web-api"
        },
        events: [],
        resource: %{"service.name" => "web-api"},
        instrumentation_scope: nil
      },
      # Trace 2: DHCP transaction with error
      %{
        trace_id: trace_id_2,
        span_id: "aaaa bbbbccccdddd" |> String.replace(" ", ""),
        parent_span_id: nil,
        name: "dhcpv4.transaction",
        kind: :internal,
        start_time: now - 300_000_000,
        end_time: now - 200_000_000,
        duration_ns: 100_000_000,
        status: :error,
        status_message: "lease allocation failed",
        attributes: %{
          "service.name" => "dhcp-server",
          "dhcp.message_type" => "DISCOVER",
          "net.sock.peer.addr" => "10.0.0.50"
        },
        events: [
          %{
            name: "dhcp.pool_exhausted",
            timestamp: now - 250_000_000,
            attributes: %{"pool" => "subnet-10", "available" => "0"}
          }
        ],
        resource: %{"service.name" => "dhcp-server"},
        instrumentation_scope: %{name: "ddnet", version: "0.14.7"}
      }
    ]

    TimelessTraces.Buffer.ingest(spans)
    TimelessTraces.flush()

    %{trace_id_1: trace_id_1, trace_id_2: trace_id_2, now: now}
  end

  # ===========================================================================
  # VictoriaTraces Jaeger API response format reference
  #
  # Services:    {"data": ["svc1", ...], "errors": null, "limit": 0, "offset": 0, "total": N}
  # Operations:  {"data": ["op1", ...],  "errors": null, "limit": 0, "offset": 0, "total": N}
  # Traces:      {"data": [{trace},...], "errors": null, "limit": 0, "offset": 0, "total": N}
  #
  # Trace:       {"traceID": "hex", "spans": [...], "processes": {"p1": {...}}, "warnings": null}
  #
  # Span:        {"traceID", "spanID", "operationName", "references": [],
  #               "startTime": int_us, "duration": int_us, "tags": [...],
  #               "logs": [...], "processID": "p1", "warnings": null}
  #
  # Tag:         {"key": "name", "type": "string"|"int64"|"float64"|"bool", "value": ...}
  # Log:         {"timestamp": int_us, "fields": [{tag}, ...]}
  # Process:     {"serviceName": "name", "tags": [{tag}, ...]}
  # Reference:   {"refType": "CHILD_OF", "traceID": "hex", "spanID": "hex"}
  # ===========================================================================

  describe "GET /select/jaeger/api/services format" do
    test "response has correct Jaeger envelope" do
      %{} = ingest_realistic_spans()

      conn = conn(:get, "/select/jaeger/api/services") |> call()
      assert conn.status == 200

      body = json_body(conn)

      # Must have full Jaeger envelope
      assert is_list(body["data"])
      assert body["errors"] in [nil, :null]
      assert body["limit"] == 0
      assert body["offset"] == 0
      assert is_integer(body["total"])
      assert body["total"] == length(body["data"])

      assert Enum.all?(body["data"], &is_binary/1)
      assert "web-api" in body["data"]
      assert "dhcp-server" in body["data"]
    end
  end

  describe "GET /select/jaeger/api/services/:service/operations format" do
    test "response has correct Jaeger envelope" do
      %{} = ingest_realistic_spans()

      conn = conn(:get, "/select/jaeger/api/services/web-api/operations") |> call()
      assert conn.status == 200

      body = json_body(conn)

      # Full Jaeger envelope
      assert is_list(body["data"])
      assert body["errors"] in [nil, :null]
      assert body["limit"] == 0
      assert body["offset"] == 0
      assert is_integer(body["total"])

      assert Enum.all?(body["data"], &is_binary/1)
      assert "GET /api/v1/users" in body["data"]
      assert "SELECT users" in body["data"]
    end

    test "returns operations that include the requested service" do
      %{} = ingest_realistic_spans()

      conn = conn(:get, "/select/jaeger/api/services/dhcp-server/operations") |> call()
      body = json_body(conn)

      assert "dhcpv4.transaction" in body["data"]
    end
  end

  describe "GET /select/jaeger/api/traces search format" do
    setup do
      ctx = ingest_realistic_spans()
      {:ok, ctx}
    end

    test "response has data array of traces", _ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      assert conn.status == 200

      body = json_body(conn)
      assert is_list(body["data"])
      assert length(body["data"]) >= 1
    end

    test "each trace has required Jaeger fields", _ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      for trace <- body["data"] do
        # traceID is a hex string
        assert is_binary(trace["traceID"])

        # spans is a non-empty list
        assert is_list(trace["spans"])
        assert length(trace["spans"]) >= 1

        # processes is a map with p1, p2, etc.
        assert is_map(trace["processes"])
        assert map_size(trace["processes"]) >= 1
      end
    end

    test "each span has all required Jaeger fields", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      assert trace != nil

      for span <- trace["spans"] do
        # Required string fields
        assert is_binary(span["traceID"]), "traceID must be string"
        assert is_binary(span["spanID"]), "spanID must be string"
        assert is_binary(span["operationName"]), "operationName must be string"
        assert is_binary(span["processID"]), "processID must be string"

        # Required integer fields (microseconds)
        assert is_integer(span["startTime"]), "startTime must be integer"
        assert is_integer(span["duration"]), "duration must be integer"
        assert span["startTime"] > 0, "startTime must be positive"
        assert span["duration"] > 0, "duration must be positive"

        # Required list fields
        assert is_list(span["tags"]), "tags must be list"
        assert is_list(span["references"]), "references must be list"
        assert is_list(span["logs"]), "logs must be list"
      end
    end

    test "timestamps are in microseconds (not nanoseconds)", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      root_span = Enum.find(trace["spans"], &(&1["operationName"] == "GET /api/v1/users"))

      # Microsecond timestamps are ~10^15, nanosecond would be ~10^18
      assert root_span["startTime"] < 10_000_000_000_000_000,
             "startTime appears to be in nanoseconds, should be microseconds"

      # Duration should be 100ms = 100_000us
      assert root_span["duration"] == 100_000
    end

    test "tags have correct Jaeger format {key, type, value}", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      root_span = Enum.find(trace["spans"], &(&1["operationName"] == "GET /api/v1/users"))

      for tag <- root_span["tags"] do
        assert is_binary(tag["key"]), "tag key must be string"

        assert tag["type"] in ["string", "int64", "float64", "bool"],
               "tag type '#{tag["type"]}' not a valid Jaeger type"

        assert Map.has_key?(tag, "value"), "tag must have value"
      end

      # Verify span.kind tag is present
      kind_tag = Enum.find(root_span["tags"], &(&1["key"] == "span.kind"))
      assert kind_tag != nil, "span.kind tag must be present"
      assert kind_tag["value"] == "server"
      assert kind_tag["type"] == "string"

      # Verify otel.status_code tag
      status_tag = Enum.find(root_span["tags"], &(&1["key"] == "otel.status_code"))
      assert status_tag != nil, "otel.status_code tag must be present"
      assert status_tag["value"] == "OK"
    end

    test "integer attribute becomes int64 tag", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      root_span = Enum.find(trace["spans"], &(&1["operationName"] == "GET /api/v1/users"))

      status_code_tag = Enum.find(root_span["tags"], &(&1["key"] == "http.status_code"))
      assert status_code_tag != nil
      assert status_code_tag["type"] == "int64"
      assert status_code_tag["value"] == 200
    end

    test "child span has CHILD_OF reference", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      child_span = Enum.find(trace["spans"], &(&1["operationName"] == "SELECT users"))

      assert length(child_span["references"]) == 1
      ref = hd(child_span["references"])
      assert ref["refType"] == "CHILD_OF"
      assert ref["traceID"] == ctx.trace_id_1
      assert ref["spanID"] == "1111222233334444"
    end

    test "root span has empty references", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      root_span = Enum.find(trace["spans"], &(&1["operationName"] == "GET /api/v1/users"))

      assert root_span["references"] == []
    end

    test "processes map has serviceName and tags", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))

      for {_key, process} <- trace["processes"] do
        assert is_binary(process["serviceName"]), "process must have serviceName string"
        assert is_list(process["tags"]), "process must have tags list"
      end

      # Verify service name is correct
      service_names =
        trace["processes"]
        |> Map.values()
        |> Enum.map(& &1["serviceName"])

      assert "web-api" in service_names
    end

    test "span events become Jaeger logs with correct format", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      root_span = Enum.find(trace["spans"], &(&1["operationName"] == "GET /api/v1/users"))

      # Root span has 1 event
      assert length(root_span["logs"]) == 1

      log = hd(root_span["logs"])
      assert is_integer(log["timestamp"]), "log timestamp must be integer"
      assert is_list(log["fields"]), "log fields must be list"

      # Timestamp should be in microseconds
      assert log["timestamp"] < 10_000_000_000_000_000,
             "log timestamp appears to be in nanoseconds"

      # Fields should include event name
      event_field = Enum.find(log["fields"], &(&1["key"] == "event"))
      assert event_field != nil, "log must have 'event' field"
      assert event_field["value"] == "request.started"

      # Fields should include attributes
      ip_field = Enum.find(log["fields"], &(&1["key"] == "client_ip"))
      assert ip_field != nil
      assert ip_field["value"] == "10.0.0.1"
    end

    test "trace has warnings field (null)", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))
      assert Map.has_key?(trace, "warnings")
      assert trace["warnings"] in [nil, :null]
    end

    test "each span has warnings field (null)", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))

      for span <- trace["spans"] do
        assert Map.has_key?(span, "warnings"), "span must have warnings field"
        assert span["warnings"] in [nil, :null]
      end
    end

    test "process tags contain resource attributes", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=web-api&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_1))

      # Find the web-api process
      web_process =
        trace["processes"]
        |> Map.values()
        |> Enum.find(&(&1["serviceName"] == "web-api"))

      assert web_process != nil
      tags = web_process["tags"]
      assert is_list(tags)
      assert length(tags) > 0

      # Resource attributes should be present as tags
      env_tag = Enum.find(tags, &(&1["key"] == "deployment.environment"))
      assert env_tag != nil
      assert env_tag["value"] == "test"
      assert env_tag["type"] == "string"

      sdk_tag = Enum.find(tags, &(&1["key"] == "telemetry.sdk.language"))
      assert sdk_tag != nil
      assert sdk_tag["value"] == "erlang"

      # service.name should NOT be in process tags (it's in serviceName)
      svc_tag = Enum.find(tags, &(&1["key"] == "service.name"))
      assert svc_tag == nil
    end

    test "error status includes status_description tag", ctx do
      conn = conn(:get, "/select/jaeger/api/traces?service=dhcp-server&limit=10") |> call()
      body = json_body(conn)

      trace = Enum.find(body["data"], &(&1["traceID"] == ctx.trace_id_2))
      assert trace != nil

      span = hd(trace["spans"])
      tags = span["tags"]

      status_tag = Enum.find(tags, &(&1["key"] == "otel.status_code"))
      assert status_tag["value"] == "ERROR"

      desc_tag = Enum.find(tags, &(&1["key"] == "otel.status_description"))
      assert desc_tag != nil
      assert desc_tag["value"] == "lease allocation failed"
    end
  end

  describe "GET /select/jaeger/api/traces/:trace_id format" do
    setup do
      ctx = ingest_realistic_spans()
      {:ok, ctx}
    end

    test "returns single trace in data array", ctx do
      conn = conn(:get, "/select/jaeger/api/traces/#{ctx.trace_id_1}") |> call()
      assert conn.status == 200

      body = json_body(conn)
      assert is_list(body["data"])
      assert length(body["data"]) == 1

      trace = hd(body["data"])
      assert trace["traceID"] == ctx.trace_id_1
    end

    test "trace contains all spans for that trace ID", ctx do
      conn = conn(:get, "/select/jaeger/api/traces/#{ctx.trace_id_1}") |> call()
      body = json_body(conn)

      trace = hd(body["data"])
      assert length(trace["spans"]) == 2

      op_names = Enum.map(trace["spans"], & &1["operationName"]) |> Enum.sort()
      assert op_names == ["GET /api/v1/users", "SELECT users"]
    end

    test "all span traceIDs match the requested trace", ctx do
      conn = conn(:get, "/select/jaeger/api/traces/#{ctx.trace_id_1}") |> call()
      body = json_body(conn)

      trace = hd(body["data"])

      for span <- trace["spans"] do
        assert span["traceID"] == ctx.trace_id_1
      end
    end

    test "nonexistent trace returns empty spans", _ctx do
      conn = conn(:get, "/select/jaeger/api/traces/00000000000000000000000000000000") |> call()
      assert conn.status == 200

      body = json_body(conn)
      assert length(body["data"]) == 1
      trace = hd(body["data"])
      assert trace["spans"] == []
    end
  end

  describe "Jaeger UI filter compatibility" do
    setup do
      ctx = ingest_realistic_spans()
      {:ok, ctx}
    end

    test "service filter works" do
      conn = conn(:get, "/select/jaeger/api/traces?service=dhcp-server&limit=10") |> call()
      body = json_body(conn)

      traces = body["data"]
      assert length(traces) >= 1

      # All returned traces should have dhcp-server
      for trace <- traces do
        services =
          trace["processes"]
          |> Map.values()
          |> Enum.map(& &1["serviceName"])

        assert "dhcp-server" in services
      end
    end

    test "limit filter works" do
      conn = conn(:get, "/select/jaeger/api/traces?limit=1") |> call()
      body = json_body(conn)

      # Should return at most 1 trace (may group spans into traces)
      assert length(body["data"]) >= 1
    end

    test "operation filter works" do
      conn =
        conn(:get, "/select/jaeger/api/traces?service=web-api&operation=SELECT+users&limit=10")
        |> call()

      body = json_body(conn)
      assert length(body["data"]) >= 1
    end
  end

  describe "OTLP ingest format compatibility" do
    test "JSON OTLP ingest matches VictoriaTraces format" do
      otlp_body =
        %{
          "resourceSpans" => [
            %{
              "resource" => %{
                "attributes" => [
                  %{"key" => "service.name", "value" => %{"stringValue" => "compat-svc"}},
                  %{
                    "key" => "deployment.environment",
                    "value" => %{"stringValue" => "production"}
                  }
                ]
              },
              "scopeSpans" => [
                %{
                  "scope" => %{"name" => "otel-compat", "version" => "1.0.0"},
                  "spans" => [
                    %{
                      "traceId" => "aabbccdd11223344aabbccdd11223344",
                      "spanId" => "1122334455667788",
                      "parentSpanId" => "",
                      "name" => "compat-operation",
                      "kind" => 2,
                      "startTimeUnixNano" => "1700000000000000000",
                      "endTimeUnixNano" => "1700000001000000000",
                      "status" => %{"code" => 1},
                      "attributes" => [
                        %{"key" => "http.method", "value" => %{"stringValue" => "POST"}},
                        %{"key" => "http.status_code", "value" => %{"intValue" => 201}}
                      ],
                      "events" => [
                        %{
                          "name" => "request.completed",
                          "timeUnixNano" => "1700000000500000000",
                          "attributes" => [
                            %{
                              "key" => "response.size",
                              "value" => %{"stringValue" => "1024"}
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
        |> :json.encode()
        |> IO.iodata_to_binary()

      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", otlp_body)
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 200
      # VictoriaTraces returns {"partialSuccess":{}}
      resp = :json.decode(conn.resp_body)
      assert resp["partialSuccess"] == %{}

      TimelessTraces.flush()

      # Now verify the Jaeger query response is valid
      conn =
        conn(:get, "/select/jaeger/api/traces/aabbccdd11223344aabbccdd11223344")
        |> call()

      body = json_body(conn)
      trace = hd(body["data"])
      span = hd(trace["spans"])

      # Verify all VictoriaTraces Jaeger fields are present
      assert span["traceID"] == "aabbccdd11223344aabbccdd11223344"
      assert span["spanID"] == "1122334455667788"
      assert span["operationName"] == "compat-operation"
      assert span["startTime"] == 1_700_000_000_000_000
      assert span["duration"] == 1_000_000

      # Tags should include the attributes
      method_tag = Enum.find(span["tags"], &(&1["key"] == "http.method"))
      assert method_tag == %{"key" => "http.method", "type" => "string", "value" => "POST"}

      status_code_tag = Enum.find(span["tags"], &(&1["key"] == "http.status_code"))

      assert status_code_tag == %{
               "key" => "http.status_code",
               "type" => "int64",
               "value" => 201
             }

      # Logs should include events
      assert length(span["logs"]) == 1
      log = hd(span["logs"])
      assert log["timestamp"] == 1_700_000_000_500_000

      event_field = Enum.find(log["fields"], &(&1["key"] == "event"))
      assert event_field["value"] == "request.completed"
    end
  end
end
