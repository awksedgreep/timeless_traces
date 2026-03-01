defmodule TimelessTraces.HTTP do
  require Logger

  @moduledoc """
  Optional HTTP interface compatible with VictoriaTraces (OTLP ingest) and Jaeger (query).

  ## Usage

  Add to your config:

      config :timeless_traces, http: true                          # port 10428, no auth
      config :timeless_traces, http: [port: 10500, bearer_token: "secret"]

  Or add to your supervision tree directly:

      children = [
        {TimelessTraces.HTTP, port: 10428}
      ]

  ## Endpoints

  ### Ingest
    * `POST /insert/opentelemetry/v1/traces` - OTLP JSON trace ingest

  ### Query (Jaeger-compatible)
    * `GET /select/jaeger/api/services` - List service names
    * `GET /select/jaeger/api/services/:service/operations` - Operations for a service
    * `GET /select/jaeger/api/traces/:trace_id` - Get full trace
    * `GET /select/jaeger/api/traces` - Search traces

  ### Operational
    * `GET /health` - Health check
    * `POST /api/v1/backup` - Online backup
    * `GET /api/v1/flush` - Force buffer flush
  """

  use Plug.Router

  @max_body_bytes 10 * 1024 * 1024

  plug(:match)
  plug(:authenticate)
  plug(:dispatch)

  def child_spec(opts) do
    port = Keyword.get(opts, :port, 10428)
    bearer_token = Keyword.get(opts, :bearer_token)
    plug_opts = [bearer_token: bearer_token]

    %{
      id: __MODULE__,
      start: {Bandit, :start_link, [[plug: {__MODULE__, plug_opts}, port: port]]},
      type: :supervisor
    }
  end

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    conn
    |> Plug.Conn.put_private(:timeless_traces_token, Keyword.get(opts, :bearer_token))
    |> super(opts)
  end

  defp authenticate(%{request_path: "/health"} = conn, _opts), do: conn

  defp authenticate(conn, _opts) do
    case conn.private[:timeless_traces_token] do
      nil -> conn
      expected -> check_token(conn, expected)
    end
  end

  defp check_token(conn, expected) do
    case extract_token(conn) do
      nil ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(401, ~s({"error":"unauthorized"}))
        |> halt()

      token ->
        if Plug.Crypto.secure_compare(token, expected) do
          conn
        else
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(403, ~s({"error":"forbidden"}))
          |> halt()
        end
    end
  end

  defp extract_token(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] ->
        String.trim(token)

      _ ->
        conn = Plug.Conn.fetch_query_params(conn)
        conn.query_params["token"]
    end
  end

  # Health check
  get "/health" do
    {:ok, stats} = TimelessTraces.stats()

    body =
      %{
        status: "ok",
        blocks: stats.total_blocks,
        spans: stats.total_entries,
        disk_size: stats.disk_size
      }
      |> :json.encode()
      |> IO.iodata_to_binary()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body)
  end

  # OTLP trace ingest (JSON + Protobuf)
  post "/insert/opentelemetry/v1/traces" do
    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, conn} ->
        content_type =
          case Plug.Conn.get_req_header(conn, "content-type") do
            [ct | _] -> ct
            _ -> "application/json"
          end

        if String.contains?(content_type, "application/x-protobuf") do
          ingest_protobuf(conn, body)
        else
          ingest_json(conn, body)
        end

      {:more, _partial, conn} ->
        body =
          %{error: "body too large", max_bytes: @max_body_bytes}
          |> :json.encode()
          |> IO.iodata_to_binary()

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(413, body)

      {:error, reason} ->
        json_error(conn, 400, to_string(reason))
    end
  end

  # List service names (Jaeger-compatible)
  get "/select/jaeger/api/services" do
    {:ok, services} = TimelessTraces.services()
    send_jaeger_response(conn, services)
  end

  # Operations for a service (Jaeger-compatible)
  get "/select/jaeger/api/services/:service/operations" do
    service = conn.path_params["service"]
    {:ok, operations} = TimelessTraces.operations(service)
    send_jaeger_response(conn, operations)
  end

  # Search traces (Jaeger-compatible)
  get "/select/jaeger/api/traces" do
    conn = Plug.Conn.fetch_query_params(conn)
    params = conn.query_params

    filters = build_trace_search_filters(params)

    case TimelessTraces.query(filters) do
      {:ok, %{entries: spans}} ->
        traces = group_spans_to_jaeger_traces(spans)
        send_jaeger_response(conn, traces)

      {:error, reason} ->
        json_error(conn, 500, inspect(reason))
    end
  end

  # Get full trace by ID (Jaeger-compatible)
  get "/select/jaeger/api/traces/:trace_id" do
    trace_id = conn.path_params["trace_id"]

    case TimelessTraces.trace(trace_id) do
      {:ok, spans} ->
        trace = spans_to_jaeger_trace(trace_id, spans)
        send_jaeger_response(conn, [trace])

      {:error, reason} ->
        json_error(conn, 500, inspect(reason))
    end
  end

  # Online backup
  post "/api/v1/backup" do
    parsed_path =
      case Plug.Conn.read_body(conn, length: 64_000) do
        {:ok, "", _} ->
          nil

        {:ok, body, _} ->
          try do
            case :json.decode(body) do
              %{"path" => path} when is_binary(path) and path != "" -> path
              _ -> nil
            end
          rescue
            _ -> nil
          end

        _ ->
          nil
      end

    target_dir = parsed_path || default_backup_dir()

    case TimelessTraces.backup(target_dir) do
      {:ok, result} ->
        body =
          %{
            status: "ok",
            path: result.path,
            files: result.files,
            total_bytes: result.total_bytes
          }
          |> :json.encode()
          |> IO.iodata_to_binary()

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)

      {:error, reason} ->
        json_error(conn, 500, inspect(reason))
    end
  end

  # Force buffer flush
  get "/api/v1/flush" do
    TimelessTraces.flush()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, :json.encode(%{status: "ok"}) |> IO.iodata_to_binary())
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # --- OTLP JSON parsing ---

  defp parse_otlp_resource_spans(resource_spans) when is_list(resource_spans) do
    Enum.flat_map(resource_spans, fn rs ->
      resource_attrs = parse_otlp_attributes(get_in(rs, ["resource", "attributes"]) || [])
      scope_spans = rs["scopeSpans"] || []

      Enum.flat_map(scope_spans, fn ss ->
        scope = parse_otlp_scope(ss["scope"])
        spans = ss["spans"] || []

        Enum.map(spans, fn span ->
          parse_otlp_span(span, resource_attrs, scope)
        end)
      end)
    end)
  end

  defp parse_otlp_resource_spans(_), do: []

  defp parse_otlp_span(span, resource_attrs, scope) do
    start_time_nanos = parse_otlp_time(span["startTimeUnixNano"])
    end_time_nanos = parse_otlp_time(span["endTimeUnixNano"])

    %{
      trace_id: span["traceId"],
      span_id: span["spanId"],
      parent_span_id: empty_to_nil(span["parentSpanId"]),
      name: span["name"] || "",
      kind: parse_otlp_kind(span["kind"]),
      start_time: start_time_nanos,
      end_time: end_time_nanos,
      duration_ns: end_time_nanos - start_time_nanos,
      status: parse_otlp_status(span["status"]),
      status_message: get_in(span, ["status", "message"]),
      attributes: parse_otlp_attributes(span["attributes"] || []),
      events: parse_otlp_events(span["events"] || []),
      resource: resource_attrs,
      instrumentation_scope: scope
    }
  end

  defp parse_otlp_time(nil), do: 0
  defp parse_otlp_time(val) when is_integer(val), do: val

  defp parse_otlp_time(val) when is_binary(val) do
    case Integer.parse(val) do
      {n, _} -> n
      :error -> 0
    end
  end

  defp parse_otlp_kind(1), do: :internal
  defp parse_otlp_kind(2), do: :server
  defp parse_otlp_kind(3), do: :client
  defp parse_otlp_kind(4), do: :producer
  defp parse_otlp_kind(5), do: :consumer
  defp parse_otlp_kind("SPAN_KIND_INTERNAL"), do: :internal
  defp parse_otlp_kind("SPAN_KIND_SERVER"), do: :server
  defp parse_otlp_kind("SPAN_KIND_CLIENT"), do: :client
  defp parse_otlp_kind("SPAN_KIND_PRODUCER"), do: :producer
  defp parse_otlp_kind("SPAN_KIND_CONSUMER"), do: :consumer
  defp parse_otlp_kind(_), do: :internal

  defp parse_otlp_status(nil), do: :unset
  defp parse_otlp_status(%{"code" => 0}), do: :unset
  defp parse_otlp_status(%{"code" => 1}), do: :ok
  defp parse_otlp_status(%{"code" => 2}), do: :error
  defp parse_otlp_status(%{"code" => "STATUS_CODE_UNSET"}), do: :unset
  defp parse_otlp_status(%{"code" => "STATUS_CODE_OK"}), do: :ok
  defp parse_otlp_status(%{"code" => "STATUS_CODE_ERROR"}), do: :error
  defp parse_otlp_status(_), do: :unset

  defp parse_otlp_attributes(attrs) when is_list(attrs) do
    Map.new(attrs, fn
      %{"key" => key, "value" => %{"stringValue" => v}} -> {key, v}
      %{"key" => key, "value" => %{"intValue" => v}} -> {key, v}
      %{"key" => key, "value" => %{"doubleValue" => v}} -> {key, v}
      %{"key" => key, "value" => %{"boolValue" => v}} -> {key, v}
      %{"key" => key, "value" => v} -> {key, inspect(v)}
      _ -> {"_unknown", ""}
    end)
  end

  defp parse_otlp_attributes(_), do: %{}

  defp parse_otlp_events(events) when is_list(events) do
    Enum.map(events, fn event ->
      %{
        name: event["name"] || "",
        timestamp: parse_otlp_time(event["timeUnixNano"]),
        attributes: parse_otlp_attributes(event["attributes"] || [])
      }
    end)
  end

  defp parse_otlp_events(_), do: []

  defp parse_otlp_scope(nil), do: nil

  defp parse_otlp_scope(scope) do
    %{
      name: scope["name"] || "",
      version: scope["version"]
    }
  end

  defp empty_to_nil(nil), do: nil
  defp empty_to_nil(""), do: nil
  defp empty_to_nil(val), do: val

  # --- Jaeger format conversion ---

  defp build_trace_search_filters(params) do
    filters = []

    filters =
      case params["service"] do
        nil -> filters
        svc -> [{:service, svc} | filters]
      end

    filters =
      case params["operation"] do
        nil -> filters
        op -> [{:name, op} | filters]
      end

    filters =
      case params["start"] do
        nil ->
          filters

        start ->
          # Jaeger sends microseconds
          case Integer.parse(start) do
            {n, _} -> [{:since, n * 1000} | filters]
            :error -> filters
          end
      end

    filters =
      case params["end"] do
        nil ->
          filters

        end_time ->
          case Integer.parse(end_time) do
            {n, _} -> [{:until, n * 1000} | filters]
            :error -> filters
          end
      end

    filters =
      case params["limit"] do
        nil ->
          filters

        limit ->
          case Integer.parse(limit) do
            {n, _} -> [{:limit, n} | filters]
            :error -> filters
          end
      end

    filters =
      case params["minDuration"] do
        nil -> filters
        dur -> [{:min_duration, parse_jaeger_duration(dur)} | filters]
      end

    filters =
      case params["maxDuration"] do
        nil -> filters
        dur -> [{:max_duration, parse_jaeger_duration(dur)} | filters]
      end

    filters
  end

  defp parse_jaeger_duration(dur) when is_binary(dur) do
    # Jaeger sends durations like "1ms", "100us", "2s"
    cond do
      String.ends_with?(dur, "ms") ->
        {n, _} = Integer.parse(String.trim_trailing(dur, "ms"))
        n * 1_000_000

      String.ends_with?(dur, "us") ->
        {n, _} = Integer.parse(String.trim_trailing(dur, "us"))
        n * 1_000

      String.ends_with?(dur, "s") ->
        {n, _} = Integer.parse(String.trim_trailing(dur, "s"))
        n * 1_000_000_000

      true ->
        case Integer.parse(dur) do
          {n, _} -> n
          :error -> 0
        end
    end
  end

  defp group_spans_to_jaeger_traces(spans) do
    spans
    |> Enum.group_by(& &1.trace_id)
    |> Enum.map(fn {trace_id, trace_spans} ->
      spans_to_jaeger_trace(trace_id, trace_spans)
    end)
  end

  defp spans_to_jaeger_trace(trace_id, spans) do
    # Build processes map (one entry per unique service, with merged resource tags)
    service_resources =
      spans
      |> Enum.reduce(%{}, fn span, acc ->
        service =
          Map.get(span.attributes, "service.name") ||
            Map.get(span.resource || %{}, "service.name") || "unknown"

        resource = span.resource || %{}
        existing = Map.get(acc, service, %{})
        Map.put(acc, service, Map.merge(existing, resource))
      end)
      |> Enum.to_list()

    indexed = Enum.with_index(service_resources, 1)

    processes =
      indexed
      |> Enum.map(fn {{service, _resource}, idx} -> {service, "p#{idx}"} end)
      |> Map.new()

    process_entries =
      Map.new(indexed, fn {{service, resource}, idx} ->
        tags =
          resource
          |> Map.drop(["service.name"])
          |> Enum.map(fn {k, v} -> attribute_to_jaeger_tag(k, v) end)

        {"p#{idx}", %{serviceName: service, tags: tags}}
      end)

    jaeger_spans =
      Enum.map(spans, fn span ->
        service =
          Map.get(span.attributes, "service.name") ||
            Map.get(span.resource || %{}, "service.name") || "unknown"

        process_id = Map.get(processes, service, "p1")

        references =
          if span.parent_span_id do
            [%{refType: "CHILD_OF", traceID: trace_id, spanID: span.parent_span_id}]
          else
            []
          end

        %{
          traceID: trace_id,
          spanID: span.span_id,
          operationName: span.name,
          references: references,
          startTime: div(span.start_time, 1000),
          duration: div(span.duration_ns, 1000),
          tags: span_to_jaeger_tags(span),
          logs: span_events_to_jaeger_logs(span.events),
          processID: process_id,
          warnings: :null
        }
      end)

    %{
      traceID: trace_id,
      spans: jaeger_spans,
      processes: process_entries,
      warnings: :null
    }
  end

  defp span_to_jaeger_tags(span) do
    base_tags = [
      %{key: "span.kind", type: "string", value: to_string(span.kind)},
      %{key: "otel.status_code", type: "string", value: String.upcase(to_string(span.status))}
    ]

    status_msg_tag =
      if span.status_message do
        [%{key: "otel.status_description", type: "string", value: span.status_message}]
      else
        []
      end

    attr_tags =
      (span.attributes || %{})
      |> Enum.map(fn {k, v} -> attribute_to_jaeger_tag(k, v) end)

    base_tags ++ status_msg_tag ++ attr_tags
  end

  defp attribute_to_jaeger_tag(key, value) when is_binary(value) do
    %{key: key, type: "string", value: value}
  end

  defp attribute_to_jaeger_tag(key, value) when is_integer(value) do
    %{key: key, type: "int64", value: value}
  end

  defp attribute_to_jaeger_tag(key, value) when is_float(value) do
    %{key: key, type: "float64", value: value}
  end

  defp attribute_to_jaeger_tag(key, value) when is_boolean(value) do
    %{key: key, type: "bool", value: value}
  end

  defp attribute_to_jaeger_tag(key, value) do
    %{key: key, type: "string", value: inspect(value)}
  end

  defp span_events_to_jaeger_logs(nil), do: []

  defp span_events_to_jaeger_logs(events) when is_list(events) do
    Enum.map(events, fn event ->
      fields =
        case event do
          %{attributes: attrs} when is_map(attrs) ->
            [
              %{key: "event", type: "string", value: event[:name] || ""}
              | Enum.map(attrs, fn {k, v} -> attribute_to_jaeger_tag(k, v) end)
            ]

          _ ->
            [%{key: "event", type: "string", value: event[:name] || ""}]
        end

      timestamp =
        case event do
          %{timestamp: ts} when is_integer(ts) -> div(ts, 1000)
          _ -> 0
        end

      %{timestamp: timestamp, fields: List.flatten(fields)}
    end)
  end

  defp send_jaeger_response(conn, data) do
    total = if is_list(data), do: length(data), else: 0

    body =
      %{data: data, errors: :null, limit: 0, offset: 0, total: total}
      |> :json.encode()
      |> IO.iodata_to_binary()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body)
  end

  defp default_backup_dir do
    data_dir = TimelessTraces.Config.data_dir()
    Path.join([data_dir, "backups", to_string(System.os_time(:second))])
  end

  defp json_error(conn, status, msg) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, :json.encode(%{error: msg}) |> IO.iodata_to_binary())
  end

  # --- JSON ingest path ---

  defp ingest_json(conn, body) do
    try do
      case :json.decode(body) do
        %{"resourceSpans" => resource_spans} ->
          spans = parse_otlp_resource_spans(resource_spans)

          if spans != [] do
            TimelessTraces.Buffer.ingest(spans)
          end

          send_resp(conn, 200, ~s({"partialSuccess":{}}))

        _ ->
          json_error(conn, 400, "missing resourceSpans field")
      end
    rescue
      _ -> json_error(conn, 400, "invalid JSON")
    end
  end

  # --- Protobuf ingest path ---

  defp ingest_protobuf(conn, body) do
    body = maybe_gunzip(conn, body)

    try do
      msg =
        :opentelemetry_exporter_trace_service_pb.decode_msg(body, :export_trace_service_request)

      resource_spans = Map.get(msg, :resource_spans, [])
      spans = parse_protobuf_resource_spans(resource_spans)

      if spans != [] do
        TimelessTraces.Buffer.ingest(spans)
      end

      send_resp(conn, 200, ~s({"partialSuccess":{}}))
    rescue
      e ->
        Logger.warning("Protobuf decode error: #{inspect(e)}")
        json_error(conn, 400, "invalid protobuf")
    end
  end

  defp maybe_gunzip(conn, body) do
    case Plug.Conn.get_req_header(conn, "content-encoding") do
      ["gzip" | _] -> :zlib.gunzip(body)
      _ -> body
    end
  end

  # --- Protobuf OTLP parsing ---

  defp parse_protobuf_resource_spans(resource_spans) when is_list(resource_spans) do
    Enum.flat_map(resource_spans, fn rs ->
      resource = Map.get(rs, :resource, %{})
      resource_attrs = parse_pb_attributes(Map.get(resource, :attributes, []))
      scope_spans = Map.get(rs, :scope_spans, [])

      Enum.flat_map(scope_spans, fn ss ->
        scope = parse_pb_scope(Map.get(ss, :scope))
        spans = Map.get(ss, :spans, [])

        Enum.map(spans, fn span ->
          parse_pb_span(span, resource_attrs, scope)
        end)
      end)
    end)
  end

  defp parse_protobuf_resource_spans(_), do: []

  defp parse_pb_span(span, resource_attrs, scope) do
    start_time_nanos = Map.get(span, :start_time_unix_nano, 0)
    end_time_nanos = Map.get(span, :end_time_unix_nano, 0)

    %{
      trace_id: encode_hex(Map.get(span, :trace_id, <<>>)),
      span_id: encode_hex(Map.get(span, :span_id, <<>>)),
      parent_span_id: encode_hex_or_nil(Map.get(span, :parent_span_id, <<>>)),
      name: Map.get(span, :name, ""),
      kind: parse_pb_kind(Map.get(span, :kind)),
      start_time: start_time_nanos,
      end_time: end_time_nanos,
      duration_ns: end_time_nanos - start_time_nanos,
      status: parse_pb_status(Map.get(span, :status)),
      status_message: get_in_status_message(Map.get(span, :status)),
      attributes: parse_pb_attributes(Map.get(span, :attributes, [])),
      events: parse_pb_events(Map.get(span, :events, [])),
      resource: resource_attrs,
      instrumentation_scope: scope
    }
  end

  defp encode_hex(<<>>), do: ""
  defp encode_hex(bin) when is_binary(bin), do: Base.encode16(bin, case: :lower)
  defp encode_hex(_), do: ""

  defp encode_hex_or_nil(<<>>), do: nil

  defp encode_hex_or_nil(bin) when is_binary(bin) and byte_size(bin) > 0 do
    Base.encode16(bin, case: :lower)
  end

  defp encode_hex_or_nil(_), do: nil

  defp parse_pb_kind(:SPAN_KIND_INTERNAL), do: :internal
  defp parse_pb_kind(:SPAN_KIND_SERVER), do: :server
  defp parse_pb_kind(:SPAN_KIND_CLIENT), do: :client
  defp parse_pb_kind(:SPAN_KIND_PRODUCER), do: :producer
  defp parse_pb_kind(:SPAN_KIND_CONSUMER), do: :consumer
  defp parse_pb_kind(_), do: :internal

  defp parse_pb_status(nil), do: :unset
  defp parse_pb_status(%{code: :STATUS_CODE_UNSET}), do: :unset
  defp parse_pb_status(%{code: :STATUS_CODE_OK}), do: :ok
  defp parse_pb_status(%{code: :STATUS_CODE_ERROR}), do: :error
  defp parse_pb_status(_), do: :unset

  defp get_in_status_message(nil), do: nil
  defp get_in_status_message(%{message: msg}) when is_binary(msg) and msg != "", do: msg
  defp get_in_status_message(_), do: nil

  defp parse_pb_attributes(attrs) when is_list(attrs) do
    Map.new(attrs, fn kv ->
      key = Map.get(kv, :key, "_unknown")
      val = extract_pb_value(Map.get(kv, :value, %{}))
      {key, val}
    end)
  end

  defp parse_pb_attributes(_), do: %{}

  defp extract_pb_value(%{value: {:string_value, v}}), do: v
  defp extract_pb_value(%{value: {:int_value, v}}), do: v
  defp extract_pb_value(%{value: {:double_value, v}}), do: v
  defp extract_pb_value(%{value: {:bool_value, v}}), do: v
  defp extract_pb_value(%{value: {:bytes_value, v}}), do: Base.encode64(v)
  defp extract_pb_value(other), do: inspect(other)

  defp parse_pb_events(events) when is_list(events) do
    Enum.map(events, fn event ->
      %{
        name: Map.get(event, :name, ""),
        timestamp: Map.get(event, :time_unix_nano, 0),
        attributes: parse_pb_attributes(Map.get(event, :attributes, []))
      }
    end)
  end

  defp parse_pb_events(_), do: []

  defp parse_pb_scope(nil), do: nil

  defp parse_pb_scope(scope) do
    %{
      name: Map.get(scope, :name, ""),
      version: Map.get(scope, :version)
    }
  end
end
