defmodule TimelessTraces.BufferShard do
  @moduledoc false

  @spec count() :: pos_integer()
  def count do
    TimelessTraces.Config.ingest_shard_count()
  end

  @spec name(non_neg_integer()) :: atom()
  def name(shard) do
    String.to_atom("timeless_traces_buffer_#{shard}")
  end

  @spec shard_for(map()) :: non_neg_integer()
  def shard_for(span) do
    shard_key(span)
    |> :erlang.phash2(count())
  end

  defp shard_key(%{trace_id: trace_id}) when is_binary(trace_id) and byte_size(trace_id) > 0,
    do: trace_id

  defp shard_key(%{attributes: attributes} = span) when is_map(attributes) do
    Map.get(attributes, "service.name") || Map.get(span, :name) || :erlang.phash2(span)
  end

  defp shard_key(span), do: Map.get(span, :name) || :erlang.phash2(span)
end
