defmodule TimelessTraces.BufferShard do
  @moduledoc false

  @spec count() :: pos_integer()
  def count do
    TimelessTraces.Config.ingest_shard_count()
  end

  @spec name(non_neg_integer()) :: {:via, Registry, {module(), term()}}
  def name(shard) do
    {:via, Registry, {TimelessTraces.ProcessRegistry, {:buffer, shard}}}
  end

  @spec shard_for(map()) :: non_neg_integer()
  def shard_for(span), do: shard_for(span, count())

  @doc false
  @spec shard_for(map(), pos_integer()) :: non_neg_integer()
  def shard_for(span, shard_count) do
    shard_key(span)
    |> :erlang.phash2(shard_count)
  end

  defp shard_key(%{trace_id: trace_id}) when is_binary(trace_id) and byte_size(trace_id) > 0,
    do: trace_id

  defp shard_key(%{attributes: attributes} = span) when is_map(attributes) do
    Map.get(attributes, "service.name") || Map.get(span, :name) || :unknown
  end

  defp shard_key(span), do: Map.get(span, :name) || :unknown
end
