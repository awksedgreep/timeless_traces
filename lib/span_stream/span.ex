defmodule SpanStream.Span do
  @moduledoc """
  A single OpenTelemetry span.
  """

  defstruct [
    :trace_id,
    :span_id,
    :parent_span_id,
    :name,
    :kind,
    :start_time,
    :end_time,
    :duration_ns,
    :status,
    :status_message,
    :attributes,
    :events,
    :resource,
    :instrumentation_scope
  ]

  @type t :: %__MODULE__{
          trace_id: String.t(),
          span_id: String.t(),
          parent_span_id: String.t() | nil,
          name: String.t(),
          kind: :internal | :server | :client | :producer | :consumer,
          start_time: integer(),
          end_time: integer(),
          duration_ns: integer(),
          status: :ok | :error | :unset,
          status_message: String.t() | nil,
          attributes: map(),
          events: list(),
          resource: map(),
          instrumentation_scope: map() | nil
        }

  @spec from_map(map()) :: t()
  def from_map(map) when is_map(map) do
    start_time = map[:start_time] || map["start_time"]
    end_time = map[:end_time] || map["end_time"]

    duration_ns =
      map[:duration_ns] || map["duration_ns"] ||
        if start_time && end_time, do: end_time - start_time, else: 0

    %__MODULE__{
      trace_id: map[:trace_id] || map["trace_id"],
      span_id: map[:span_id] || map["span_id"],
      parent_span_id: map[:parent_span_id] || map["parent_span_id"],
      name: to_string(map[:name] || map["name"] || ""),
      kind: normalize_kind(map[:kind] || map["kind"] || :internal),
      start_time: start_time,
      end_time: end_time,
      duration_ns: duration_ns,
      status: normalize_status(map[:status] || map["status"] || :unset),
      status_message: map[:status_message] || map["status_message"],
      attributes: map[:attributes] || map["attributes"] || %{},
      events: map[:events] || map["events"] || [],
      resource: map[:resource] || map["resource"] || %{},
      instrumentation_scope: map[:instrumentation_scope] || map["instrumentation_scope"]
    }
  end

  @spec encode_trace_id(non_neg_integer()) :: String.t()
  def encode_trace_id(id) when is_integer(id) do
    id
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(32, "0")
  end

  def encode_trace_id(id) when is_binary(id), do: id

  @spec encode_span_id(non_neg_integer()) :: String.t()
  def encode_span_id(id) when is_integer(id) do
    id
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(16, "0")
  end

  def encode_span_id(id) when is_binary(id), do: id

  @spec normalize_kind(atom() | String.t()) :: atom()
  def normalize_kind(:internal), do: :internal
  def normalize_kind(:server), do: :server
  def normalize_kind(:client), do: :client
  def normalize_kind(:producer), do: :producer
  def normalize_kind(:consumer), do: :consumer
  def normalize_kind(:SPAN_KIND_INTERNAL), do: :internal
  def normalize_kind(:SPAN_KIND_SERVER), do: :server
  def normalize_kind(:SPAN_KIND_CLIENT), do: :client
  def normalize_kind(:SPAN_KIND_PRODUCER), do: :producer
  def normalize_kind(:SPAN_KIND_CONSUMER), do: :consumer
  def normalize_kind(kind) when is_binary(kind), do: normalize_kind(String.to_existing_atom(kind))
  def normalize_kind(_), do: :internal

  @spec normalize_status(atom() | String.t()) :: atom()
  def normalize_status(:ok), do: :ok
  def normalize_status(:error), do: :error
  def normalize_status(:unset), do: :unset
  def normalize_status("ok"), do: :ok
  def normalize_status("error"), do: :error
  def normalize_status(_), do: :unset

  @spec normalize_attributes(term()) :: map()
  def normalize_attributes(attrs) when is_map(attrs) do
    Map.new(attrs, fn {k, v} -> {to_string(k), normalize_attr_value(v)} end)
  end

  def normalize_attributes(attrs) when is_list(attrs) do
    Map.new(attrs, fn {k, v} -> {to_string(k), normalize_attr_value(v)} end)
  end

  def normalize_attributes(_), do: %{}

  defp normalize_attr_value(v) when is_binary(v), do: v
  defp normalize_attr_value(v) when is_atom(v), do: Atom.to_string(v)
  defp normalize_attr_value(v) when is_number(v), do: v
  defp normalize_attr_value(v) when is_boolean(v), do: v
  defp normalize_attr_value(v), do: inspect(v)
end
