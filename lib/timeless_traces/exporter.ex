defmodule TimelessTraces.Exporter do
  @moduledoc """
  OpenTelemetry traces exporter that stores spans in TimelessTraces.

  ## Configuration

      config :opentelemetry,
        traces_exporter: {TimelessTraces.Exporter, []}
  """

  @behaviour :otel_exporter_traces

  @impl true
  def init(_config) do
    {:ok, %{}}
  end

  @impl true
  def export(tab, resource, state) do
    resource_map = normalize_resource(resource)

    spans =
      :ets.tab2list(tab)
      |> Enum.map(fn record -> span_from_record(record, resource_map) end)
      |> Enum.reject(&is_nil/1)

    if spans != [] do
      TimelessTraces.Buffer.ingest(spans)
    end

    {:ok, state}
  end

  @impl true
  def shutdown(_state) do
    :ok
  end

  defp span_from_record(record, resource_map) do
    try do
      # The ETS table contains span records as tuples.
      # Record format (opentelemetry ~> 1.5):
      #   {:span, trace_id, span_id, tracestate, parent_span_id,
      #    parent_span_is_remote, name, kind, start_time, end_time,
      #    attributes, events, links, status, trace_flags,
      #    is_recording, instrumentation_scope}
      case record do
        {:span, trace_id, span_id, _tracestate, parent_span_id, _parent_span_is_remote, name,
         kind, start_time, end_time, attributes, events, links, status, _trace_flags,
         _is_recording, instrumentation_scope} ->
          %{
            trace_id: TimelessTraces.Span.encode_trace_id(trace_id),
            span_id: TimelessTraces.Span.encode_span_id(span_id),
            parent_span_id: normalize_parent_span_id(parent_span_id),
            name: to_string(name),
            kind: TimelessTraces.Span.normalize_kind(kind),
            start_time: :opentelemetry.convert_timestamp(start_time, :nanosecond),
            end_time: :opentelemetry.convert_timestamp(end_time, :nanosecond),
            duration_ns: :erlang.convert_time_unit(end_time - start_time, :native, :nanosecond),
            status: extract_status(status),
            status_message: extract_status_message(status),
            attributes: TimelessTraces.Span.normalize_attributes(extract_attributes(attributes)),
            events: normalize_events(events),
            links: normalize_links(links),
            resource: resource_map,
            instrumentation_scope: normalize_scope(instrumentation_scope)
          }

        _ ->
          nil
      end
    rescue
      _ -> nil
    end
  end

  defp normalize_parent_span_id(:undefined), do: nil
  defp normalize_parent_span_id(0), do: nil
  defp normalize_parent_span_id(id) when is_integer(id), do: TimelessTraces.Span.encode_span_id(id)
  defp normalize_parent_span_id(id), do: id

  defp extract_status({:status, code, message}) do
    case code do
      :ok -> :ok
      :error -> :error
      _ -> :unset
    end
    |> then(fn s -> if message != "" and message != :undefined, do: s, else: s end)
  end

  defp extract_status(_), do: :unset

  defp extract_status_message({:status, _code, message})
       when is_binary(message) and message != "",
       do: message

  defp extract_status_message(_), do: nil

  defp extract_attributes(attrs) when is_map(attrs), do: attrs

  defp extract_attributes(attrs) when is_list(attrs) do
    Map.new(attrs, fn {k, v} -> {k, v} end)
  end

  defp extract_attributes({:attributes, _, _, _, attrs}) when is_list(attrs) do
    Map.new(attrs, fn {k, v} -> {k, v} end)
  end

  defp extract_attributes({:attributes, _, _, _, attrs}) when is_map(attrs), do: attrs
  defp extract_attributes(_), do: %{}

  defp normalize_events(events) when is_list(events), do: events
  defp normalize_events({:events, _, _, _, events}) when is_list(events), do: events
  defp normalize_events(_), do: []

  defp normalize_links(links) when is_list(links), do: links
  defp normalize_links({:links, _, _, _, links}) when is_list(links), do: links
  defp normalize_links(_), do: []

  defp normalize_resource(resource) do
    try do
      case :otel_resource.attributes(resource) do
        attrs when is_map(attrs) ->
          Map.new(attrs, fn {k, v} -> {to_string(k), to_string(v)} end)

        attrs when is_list(attrs) ->
          Map.new(attrs, fn {k, v} -> {to_string(k), to_string(v)} end)

        _ ->
          %{}
      end
    rescue
      _ -> %{}
    end
  end

  defp normalize_scope({:instrumentation_scope, name, version, _schema_url, _attributes}) do
    %{
      name: to_string(name),
      version: if(version == :undefined, do: nil, else: to_string(version))
    }
  end

  defp normalize_scope(_), do: nil
end
