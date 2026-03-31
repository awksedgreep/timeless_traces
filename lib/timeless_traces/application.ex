defmodule TimelessTraces.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    storage = TimelessTraces.Config.storage()
    data_dir = TimelessTraces.Config.data_dir()

    if storage == :disk do
      blocks_dir = Path.join(data_dir, "blocks")
      File.mkdir_p!(blocks_dir)
    end

    children =
      [
        {Registry, keys: :duplicate, name: TimelessTraces.Registry},
        {TimelessTraces.DB,
         name: TimelessTraces.DB, data_dir: data_dir, clean: storage == :memory},
        {TimelessTraces.Index, data_dir: data_dir, storage: storage, db: TimelessTraces.DB},
        {Task.Supervisor, name: TimelessTraces.FlushSupervisor},
        {TimelessTraces.Compactor, data_dir: data_dir, storage: storage},
        {TimelessTraces.Retention, []}
      ] ++ buffer_shards(data_dir) ++ http_child()

    opts = [strategy: :one_for_one, name: TimelessTraces.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp http_child do
    case Application.get_env(:timeless_traces, :http, false) do
      false -> []
      true -> [{TimelessTraces.HTTP, []}]
      opts when is_list(opts) -> [{TimelessTraces.HTTP, opts}]
    end
  end

  defp buffer_shards(data_dir) do
    for shard <- 0..(TimelessTraces.BufferShard.count() - 1) do
      Supervisor.child_spec(
        {TimelessTraces.Buffer,
         data_dir: data_dir, shard: shard, name: TimelessTraces.BufferShard.name(shard)},
        id: {:timeless_traces_buffer, shard}
      )
    end
  end
end
