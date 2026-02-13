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
        {TimelessTraces.Index, data_dir: data_dir, storage: storage},
        {TimelessTraces.Buffer, data_dir: data_dir},
        {TimelessTraces.Compactor, data_dir: data_dir, storage: storage},
        {TimelessTraces.Retention, []}
      ] ++ http_child()

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
end
