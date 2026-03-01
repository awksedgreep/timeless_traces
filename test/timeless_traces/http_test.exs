defmodule TimelessTraces.HTTPTest do
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  setup do
    Application.stop(:timeless_traces)
    Application.put_env(:timeless_traces, :storage, :memory)
    Application.put_env(:timeless_traces, :data_dir, "test/tmp/http_should_not_exist")
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

  defp call(conn, opts \\ []) do
    TimelessTraces.HTTP.call(conn, TimelessTraces.HTTP.init(opts))
  end

  defp json_body(conn) do
    :json.decode(conn.resp_body)
  end

  defp json_encode(term) do
    term |> :json.encode() |> IO.iodata_to_binary()
  end

  defp make_span(overrides \\ %{}) do
    now = System.system_time(:nanosecond)

    Map.merge(
      %{
        trace_id: "abc#{System.unique_integer([:positive])}def",
        span_id: "span#{System.unique_integer([:positive])}",
        parent_span_id: nil,
        name: "HTTP GET /api",
        kind: :server,
        start_time: now,
        end_time: now + 50_000_000,
        duration_ns: 50_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET", "service.name" => "my-app"},
        events: [],
        resource: %{"service.name" => "my-app"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  defp ingest_and_flush(spans) do
    TimelessTraces.Buffer.ingest(spans)
    TimelessTraces.flush()
  end

  # --- Health ---

  describe "GET /health" do
    test "returns ok with stats" do
      conn = conn(:get, "/health") |> call()

      assert conn.status == 200
      body = json_body(conn)
      assert body["status"] == "ok"
      assert is_integer(body["blocks"])
      assert is_integer(body["spans"])
      assert is_integer(body["disk_size"])
    end
  end

  # --- OTLP Ingest ---

  describe "POST /insert/opentelemetry/v1/traces" do
    test "ingests OTLP JSON with resourceSpans" do
      otlp_body =
        json_encode(%{
          resourceSpans: [
            %{
              resource: %{
                attributes: [
                  %{key: "service.name", value: %{stringValue: "test-svc"}}
                ]
              },
              scopeSpans: [
                %{
                  scope: %{name: "my-lib", version: "1.0"},
                  spans: [
                    %{
                      traceId: "aaaabbbbccccdddd1111222233334444",
                      spanId: "1111222233334444",
                      parentSpanId: "",
                      name: "GET /users",
                      kind: 2,
                      startTimeUnixNano: "1700000000000000000",
                      endTimeUnixNano: "1700000050000000",
                      status: %{code: 1},
                      attributes: [
                        %{key: "http.method", value: %{stringValue: "GET"}},
                        %{key: "http.status_code", value: %{intValue: 200}}
                      ],
                      events: []
                    },
                    %{
                      traceId: "aaaabbbbccccdddd1111222233334444",
                      spanId: "5555666677778888",
                      parentSpanId: "1111222233334444",
                      name: "DB query",
                      kind: 3,
                      startTimeUnixNano: "1700000010000000000",
                      endTimeUnixNano: "1700000040000000000",
                      status: %{code: 0},
                      attributes: [
                        %{key: "db.system", value: %{stringValue: "postgresql"}}
                      ],
                      events: []
                    }
                  ]
                }
              ]
            }
          ]
        })

      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", otlp_body)
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 200

      TimelessTraces.flush()

      {:ok, %{entries: spans}} = TimelessTraces.query([])
      assert length(spans) == 2

      names = Enum.map(spans, & &1.name) |> Enum.sort()
      assert names == ["DB query", "GET /users"]
    end

    test "ingests protobuf from real OpenTelemetry exporter" do
      # Build a protobuf ExportTraceServiceRequest exactly as the Erlang exporter would
      pb_msg = %{
        resource_spans: [
          %{
            resource: %{
              attributes: [
                %{key: "service.name", value: %{value: {:string_value, "pb-test-svc"}}}
              ]
            },
            scope_spans: [
              %{
                scope: %{name: "otel-test", version: "0.1"},
                spans: [
                  %{
                    trace_id: <<0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44,
                                0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xAA, 0xBB>>,
                    span_id: <<0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88>>,
                    parent_span_id: <<>>,
                    name: "GET /protobuf-test",
                    kind: :SPAN_KIND_SERVER,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_001_000_000_000,
                    status: %{code: :STATUS_CODE_OK, message: ""},
                    attributes: [
                      %{key: "http.method", value: %{value: {:string_value, "GET"}}},
                      %{key: "http.status_code", value: %{value: {:int_value, 200}}},
                      %{key: "http.duration_ms", value: %{value: {:double_value, 42.5}}},
                      %{key: "http.ok", value: %{value: {:bool_value, true}}}
                    ],
                    events: [],
                    links: []
                  },
                  %{
                    trace_id: <<0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44,
                                0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xAA, 0xBB>>,
                    span_id: <<0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00>>,
                    parent_span_id: <<0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88>>,
                    name: "DB query",
                    kind: :SPAN_KIND_CLIENT,
                    start_time_unix_nano: 1_700_000_000_100_000_000,
                    end_time_unix_nano: 1_700_000_000_900_000_000,
                    status: %{code: :STATUS_CODE_UNSET, message: ""},
                    attributes: [
                      %{key: "db.system", value: %{value: {:string_value, "postgresql"}}}
                    ],
                    events: [],
                    links: []
                  }
                ]
              }
            ]
          }
        ]
      }

      body =
        :opentelemetry_exporter_trace_service_pb.encode_msg(
          pb_msg,
          :export_trace_service_request
        )

      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", body)
        |> put_req_header("content-type", "application/x-protobuf")
        |> call()

      assert conn.status == 200

      TimelessTraces.flush()

      {:ok, %{entries: spans}} = TimelessTraces.query([])
      assert length(spans) == 2

      names = Enum.map(spans, & &1.name) |> Enum.sort()
      assert names == ["DB query", "GET /protobuf-test"]

      root = Enum.find(spans, &(&1.name == "GET /protobuf-test"))
      assert root.trace_id == "aabbccdd11223344556677889900aabb"
      assert root.span_id == "1122334455667788"
      assert root.parent_span_id == nil
      assert root.kind == :server
      assert root.status == :ok
      assert root.attributes["http.method"] == "GET"
      assert root.attributes["http.status_code"] == 200
      assert root.attributes["http.duration_ms"] == 42.5
      assert root.attributes["http.ok"] == true
      assert root.duration_ns == 1_000_000_000

      child = Enum.find(spans, &(&1.name == "DB query"))
      assert child.parent_span_id == "1122334455667788"
      assert child.kind == :client
      assert child.status == :unset
      assert child.attributes["db.system"] == "postgresql"
    end

    test "ingests gzip-compressed protobuf" do
      pb_msg = %{
        resource_spans: [
          %{
            resource: %{
              attributes: [
                %{key: "service.name", value: %{value: {:string_value, "gzip-svc"}}}
              ]
            },
            scope_spans: [
              %{
                scope: %{name: "test", version: "1.0"},
                spans: [
                  %{
                    trace_id: <<1::128>>,
                    span_id: <<2::64>>,
                    parent_span_id: <<>>,
                    name: "gzip-span",
                    kind: :SPAN_KIND_INTERNAL,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_001_000_000_000,
                    status: %{code: :STATUS_CODE_OK, message: ""},
                    attributes: [],
                    events: [],
                    links: []
                  }
                ]
              }
            ]
          }
        ]
      }

      body =
        :opentelemetry_exporter_trace_service_pb.encode_msg(
          pb_msg,
          :export_trace_service_request
        )

      gzipped = :zlib.gzip(body)

      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", gzipped)
        |> put_req_header("content-type", "application/x-protobuf")
        |> put_req_header("content-encoding", "gzip")
        |> call()

      assert conn.status == 200

      TimelessTraces.flush()

      {:ok, %{entries: [span]}} = TimelessTraces.query([])
      assert span.name == "gzip-span"
    end

    test "protobuf span kind mapping" do
      for {pb_kind, expected} <- [
            {:SPAN_KIND_INTERNAL, :internal},
            {:SPAN_KIND_SERVER, :server},
            {:SPAN_KIND_CLIENT, :client},
            {:SPAN_KIND_PRODUCER, :producer},
            {:SPAN_KIND_CONSUMER, :consumer}
          ] do
        Application.stop(:timeless_traces)
        Application.ensure_all_started(:timeless_traces)

        pb_msg = %{
          resource_spans: [
            %{
              resource: %{attributes: []},
              scope_spans: [
                %{
                  spans: [
                    %{
                      trace_id: :crypto.strong_rand_bytes(16),
                      span_id: :crypto.strong_rand_bytes(8),
                      parent_span_id: <<>>,
                      name: "kind-test",
                      kind: pb_kind,
                      start_time_unix_nano: 1_700_000_000_000_000_000,
                      end_time_unix_nano: 1_700_000_001_000_000_000,
                      status: %{code: :STATUS_CODE_UNSET, message: ""},
                      attributes: [],
                      events: [],
                      links: []
                    }
                  ]
                }
              ]
            }
          ]
        }

        body =
          :opentelemetry_exporter_trace_service_pb.encode_msg(
            pb_msg,
            :export_trace_service_request
          )

        conn(:post, "/insert/opentelemetry/v1/traces", body)
        |> put_req_header("content-type", "application/x-protobuf")
        |> call()

        TimelessTraces.flush()

        {:ok, %{entries: [span]}} = TimelessTraces.query([])

        assert span.kind == expected,
               "Expected kind #{expected} for protobuf kind #{pb_kind}, got #{span.kind}"
      end
    end

    test "protobuf status code mapping" do
      for {pb_code, expected} <- [
            {:STATUS_CODE_UNSET, :unset},
            {:STATUS_CODE_OK, :ok},
            {:STATUS_CODE_ERROR, :error}
          ] do
        Application.stop(:timeless_traces)
        Application.ensure_all_started(:timeless_traces)

        pb_msg = %{
          resource_spans: [
            %{
              resource: %{attributes: []},
              scope_spans: [
                %{
                  spans: [
                    %{
                      trace_id: :crypto.strong_rand_bytes(16),
                      span_id: :crypto.strong_rand_bytes(8),
                      parent_span_id: <<>>,
                      name: "status-test",
                      kind: :SPAN_KIND_INTERNAL,
                      start_time_unix_nano: 1_700_000_000_000_000_000,
                      end_time_unix_nano: 1_700_000_001_000_000_000,
                      status: %{code: pb_code, message: ""},
                      attributes: [],
                      events: [],
                      links: []
                    }
                  ]
                }
              ]
            }
          ]
        }

        body =
          :opentelemetry_exporter_trace_service_pb.encode_msg(
            pb_msg,
            :export_trace_service_request
          )

        conn(:post, "/insert/opentelemetry/v1/traces", body)
        |> put_req_header("content-type", "application/x-protobuf")
        |> call()

        TimelessTraces.flush()

        {:ok, %{entries: [span]}} = TimelessTraces.query([])

        assert span.status == expected,
               "Expected status #{expected} for protobuf code #{pb_code}, got #{span.status}"
      end
    end

    test "returns error for missing resourceSpans" do
      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", ~s({"data": "nope"}))
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 400
      assert json_body(conn)["error"] =~ "resourceSpans"
    end

    test "returns error for invalid JSON" do
      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", "not json")
        |> put_req_header("content-type", "application/json")
        |> call()

      assert conn.status == 400
      assert json_body(conn)["error"] =~ "invalid JSON"
    end

    test "returns error for invalid protobuf" do
      conn =
        conn(:post, "/insert/opentelemetry/v1/traces", "not protobuf at all")
        |> put_req_header("content-type", "application/x-protobuf")
        |> call()

      assert conn.status == 400
      assert json_body(conn)["error"] =~ "protobuf"
    end
  end

  # --- Jaeger Services ---

  describe "GET /select/jaeger/api/services" do
    test "returns empty list when no data" do
      conn = conn(:get, "/select/jaeger/api/services") |> call()

      assert conn.status == 200
      assert json_body(conn)["data"] == []
    end

    test "returns distinct service names" do
      spans = [
        make_span(%{
          attributes: %{"service.name" => "alpha"},
          resource: %{"service.name" => "alpha"}
        }),
        make_span(%{
          attributes: %{"service.name" => "beta"},
          resource: %{"service.name" => "beta"}
        }),
        make_span(%{
          attributes: %{"service.name" => "alpha"},
          resource: %{"service.name" => "alpha"}
        })
      ]

      ingest_and_flush(spans)

      conn = conn(:get, "/select/jaeger/api/services") |> call()
      assert conn.status == 200

      services = json_body(conn)["data"]
      assert Enum.sort(services) == ["alpha", "beta"]
    end
  end

  # --- Jaeger Operations ---

  describe "GET /select/jaeger/api/services/:service/operations" do
    test "returns operations for a service" do
      api_spans = [
        make_span(%{
          name: "GET /users",
          attributes: %{"service.name" => "api"},
          resource: %{"service.name" => "api"}
        }),
        make_span(%{
          name: "POST /users",
          attributes: %{"service.name" => "api"},
          resource: %{"service.name" => "api"}
        })
      ]

      ingest_and_flush(api_spans)

      monitor_spans = [
        make_span(%{
          name: "GET /health",
          attributes: %{"service.name" => "monitor"},
          resource: %{"service.name" => "monitor"}
        })
      ]

      ingest_and_flush(monitor_spans)

      conn = conn(:get, "/select/jaeger/api/services/api/operations") |> call()
      assert conn.status == 200

      ops = json_body(conn)["data"]
      assert "GET /users" in ops
      assert "POST /users" in ops
      refute "GET /health" in ops
    end
  end

  # --- Jaeger Trace Search ---

  describe "GET /select/jaeger/api/traces" do
    test "returns traces matching service filter" do
      trace_id = "search#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "root1",
          name: "GET /api",
          attributes: %{"service.name" => "web", "http.method" => "GET"},
          resource: %{"service.name" => "web"},
          start_time: now,
          end_time: now + 100_000_000,
          duration_ns: 100_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child1",
          parent_span_id: "root1",
          name: "DB query",
          attributes: %{"service.name" => "web", "db.system" => "postgres"},
          resource: %{"service.name" => "web"},
          start_time: now + 10_000_000,
          end_time: now + 80_000_000,
          duration_ns: 70_000_000
        })
      ]

      ingest_and_flush(spans)

      conn = conn(:get, "/select/jaeger/api/traces?service=web&limit=10") |> call()
      assert conn.status == 200

      body = json_body(conn)
      traces = body["data"]
      assert length(traces) >= 1

      trace = hd(traces)
      assert trace["traceID"] == trace_id
      assert is_list(trace["spans"])
      assert is_map(trace["processes"])
    end

    test "Jaeger trace format has correct fields" do
      trace_id = "fmt#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          name: "root-op",
          start_time: now,
          end_time: now + 50_000_000,
          duration_ns: 50_000_000
        })
      ]

      ingest_and_flush(spans)

      conn = conn(:get, "/select/jaeger/api/traces?limit=1") |> call()
      body = json_body(conn)
      trace = hd(body["data"])
      span = hd(trace["spans"])

      assert span["traceID"] == trace_id
      assert span["spanID"] == "root"
      assert span["operationName"] == "root-op"
      assert is_integer(span["startTime"])
      assert is_integer(span["duration"])
      # Jaeger uses microseconds
      assert span["startTime"] == div(now, 1000)
      assert span["duration"] == 50_000
      assert is_list(span["tags"])
      assert is_list(span["references"])
      assert is_binary(span["processID"])
    end
  end

  # --- Jaeger Get Trace ---

  describe "GET /select/jaeger/api/traces/:trace_id" do
    test "returns full trace with spans and processes" do
      trace_id = "full#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "root",
          parent_span_id: nil,
          name: "root-op",
          attributes: %{"service.name" => "frontend"},
          resource: %{"service.name" => "frontend"},
          start_time: now,
          end_time: now + 100_000_000,
          duration_ns: 100_000_000
        }),
        make_span(%{
          trace_id: trace_id,
          span_id: "child",
          parent_span_id: "root",
          name: "db-query",
          attributes: %{"service.name" => "backend", "db.system" => "postgres"},
          resource: %{"service.name" => "backend"},
          start_time: now + 10_000_000,
          end_time: now + 80_000_000,
          duration_ns: 70_000_000
        })
      ]

      ingest_and_flush(spans)

      conn = conn(:get, "/select/jaeger/api/traces/#{trace_id}") |> call()
      assert conn.status == 200

      body = json_body(conn)
      assert length(body["data"]) == 1

      trace = hd(body["data"])
      assert trace["traceID"] == trace_id
      assert length(trace["spans"]) == 2

      # Check processes map contains services
      process_services =
        trace["processes"]
        |> Map.values()
        |> Enum.map(& &1["serviceName"])
        |> Enum.sort()

      assert "backend" in process_services
      assert "frontend" in process_services

      # Check child has CHILD_OF reference
      child_span = Enum.find(trace["spans"], &(&1["spanID"] == "child"))
      assert length(child_span["references"]) == 1
      ref = hd(child_span["references"])
      assert ref["refType"] == "CHILD_OF"
      assert ref["spanID"] == "root"
    end

    test "Jaeger tags include span kind and status" do
      trace_id = "tags#{System.unique_integer([:positive])}"
      now = System.system_time(:nanosecond)

      spans = [
        make_span(%{
          trace_id: trace_id,
          span_id: "s1",
          name: "tagged-op",
          kind: :client,
          status: :error,
          status_message: "connection refused",
          start_time: now,
          end_time: now + 50_000_000,
          duration_ns: 50_000_000
        })
      ]

      ingest_and_flush(spans)

      conn = conn(:get, "/select/jaeger/api/traces/#{trace_id}") |> call()
      trace = hd(json_body(conn)["data"])
      span = hd(trace["spans"])
      tags = span["tags"]

      kind_tag = Enum.find(tags, &(&1["key"] == "span.kind"))
      assert kind_tag["value"] == "client"

      status_tag = Enum.find(tags, &(&1["key"] == "otel.status_code"))
      assert status_tag["value"] == "ERROR"

      desc_tag = Enum.find(tags, &(&1["key"] == "otel.status_description"))
      assert desc_tag["value"] == "connection refused"
    end
  end

  # --- Flush ---

  describe "GET /api/v1/flush" do
    test "returns ok" do
      conn = conn(:get, "/api/v1/flush") |> call()
      assert conn.status == 200
      assert json_body(conn)["status"] == "ok"
    end
  end

  # --- Auth ---

  describe "bearer token auth" do
    test "401 when token required but not provided" do
      conn = conn(:get, "/select/jaeger/api/services") |> call(bearer_token: "secret")
      assert conn.status == 401
      assert json_body(conn)["error"] == "unauthorized"
    end

    test "403 when token is wrong" do
      conn =
        conn(:get, "/select/jaeger/api/services")
        |> put_req_header("authorization", "Bearer wrong")
        |> call(bearer_token: "secret")

      assert conn.status == 403
      assert json_body(conn)["error"] == "forbidden"
    end

    test "passes with correct bearer token" do
      conn =
        conn(:get, "/select/jaeger/api/services")
        |> put_req_header("authorization", "Bearer secret")
        |> call(bearer_token: "secret")

      assert conn.status == 200
    end

    test "passes with query param token" do
      conn =
        conn(:get, "/select/jaeger/api/services?token=secret")
        |> call(bearer_token: "secret")

      assert conn.status == 200
    end

    test "health endpoint skips auth" do
      conn = conn(:get, "/health") |> call(bearer_token: "secret")
      assert conn.status == 200
    end
  end

  # --- 404 ---

  describe "catch-all" do
    test "returns 404 for unknown routes" do
      conn = conn(:get, "/nonexistent") |> call()
      assert conn.status == 404
    end
  end
end
