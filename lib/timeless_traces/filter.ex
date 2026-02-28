defmodule TimelessTraces.Filter do
  @moduledoc false

  @spec filter([map()], keyword()) :: [map()]
  def filter(entries, filters) do
    Enum.filter(entries, &matches?(&1, filters))
  end

  @spec matches?(map(), keyword()) :: boolean()
  def matches?(span, filters) do
    Enum.all?(filters, fn
      {:name, pattern} ->
        downcased = String.downcase(pattern)

        String.contains?(String.downcase(to_string(span.name)), downcased) or
          Enum.any?(Map.get(span, :attributes, %{}) || %{}, fn {_k, v} ->
            is_binary(v) and String.contains?(String.downcase(v), downcased)
          end)

      {:service, service} ->
        get_service_name(span) == service

      {:kind, kind} ->
        span.kind == kind

      {:status, status} ->
        span.status == status

      {:min_duration, min_ns} ->
        (span.duration_ns || 0) >= min_ns

      {:max_duration, max_ns} ->
        (span.duration_ns || 0) <= max_ns

      {:since, ts} ->
        span.start_time >= to_nanos(ts)

      {:until, ts} ->
        span.start_time <= to_nanos(ts)

      {:trace_id, trace_id} ->
        span.trace_id == trace_id

      {:attributes, map} ->
        Enum.all?(map, fn {k, v} ->
          Map.get(span.attributes, to_string(k)) == to_string(v)
        end)

      _ ->
        true
    end)
  end

  defp get_service_name(span) do
    Map.get(span.attributes, "service.name") ||
      Map.get(span.resource, "service.name") ||
      get_in(span, [Access.key(:resource, %{}), Access.key("service.name")])
  end

  defp to_nanos(%DateTime{} = dt), do: DateTime.to_unix(dt, :nanosecond)
  defp to_nanos(ts) when is_integer(ts), do: ts
end
