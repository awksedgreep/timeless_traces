defmodule TimelessTraces.Compactor do
  @moduledoc false

  use GenServer

  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec compact_now() :: :ok | :noop
  def compact_now do
    GenServer.call(__MODULE__, :compact_now, 60_000)
  end

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, :disk)
    data_dir = Keyword.get(opts, :data_dir, TimelessTraces.Config.data_dir())
    interval = TimelessTraces.Config.compaction_interval()
    schedule(interval)

    {:ok, %{storage: storage, data_dir: data_dir, interval: interval}}
  end

  @impl true
  def handle_call(:compact_now, _from, state) do
    result = maybe_compact(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:compaction_check, state) do
    maybe_compact(state)
    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :compaction_check, interval)
  end

  defp maybe_compact(state) do
    stats = TimelessTraces.Index.raw_block_stats()
    threshold = TimelessTraces.Config.compaction_threshold()
    max_age = TimelessTraces.Config.compaction_max_raw_age()
    now = System.system_time(:second)

    age_exceeded =
      stats.oldest_created_at != nil and
        now - stats.oldest_created_at >= max_age and
        stats.block_count > 0

    if stats.entry_count >= threshold or age_exceeded do
      do_compact(state)
    else
      :noop
    end
  end

  defp do_compact(state) do
    start_time = System.monotonic_time()
    raw_blocks = TimelessTraces.Index.raw_block_ids()

    all_entries =
      Enum.flat_map(raw_blocks, fn {block_id, file_path} ->
        read_result =
          case state.storage do
            :disk -> TimelessTraces.Writer.read_block(file_path, :raw)
            :memory -> TimelessTraces.Index.read_block_data(block_id)
          end

        case read_result do
          {:ok, entries} -> entries
          {:error, _} -> []
        end
      end)

    if all_entries == [] do
      :noop
    else
      sorted = Enum.sort_by(all_entries, & &1.start_time)

      write_target = if state.storage == :memory, do: :memory, else: state.data_dir

      case TimelessTraces.Writer.write_block(sorted, write_target, :zstd) do
        {:ok, new_meta} ->
          old_ids = Enum.map(raw_blocks, &elem(&1, 0))
          TimelessTraces.Index.compact_blocks(old_ids, new_meta, sorted)

          duration = System.monotonic_time() - start_time

          TimelessTraces.Telemetry.event(
            [:timeless_traces, :compaction, :stop],
            %{
              duration: duration,
              raw_blocks: length(raw_blocks),
              entry_count: length(sorted),
              byte_size: new_meta.byte_size
            },
            %{}
          )

          :ok

        {:error, reason} ->
          Logger.warning("TimelessTraces: compaction failed: #{inspect(reason)}")
          :noop
      end
    end
  end
end
