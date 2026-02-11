defmodule SpanStream.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    storage = SpanStream.Config.storage()
    data_dir = SpanStream.Config.data_dir()

    if storage == :disk do
      blocks_dir = Path.join(data_dir, "blocks")
      File.mkdir_p!(blocks_dir)
    end

    children = [
      {Registry, keys: :duplicate, name: SpanStream.Registry},
      {SpanStream.Index, data_dir: data_dir, storage: storage},
      {SpanStream.Buffer, data_dir: data_dir},
      {SpanStream.Compactor, data_dir: data_dir, storage: storage},
      {SpanStream.Retention, []}
    ]

    opts = [strategy: :one_for_one, name: SpanStream.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
