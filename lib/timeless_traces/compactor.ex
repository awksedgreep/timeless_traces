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

  @spec merge_now() :: :ok | :noop
  def merge_now do
    GenServer.call(__MODULE__, :merge_now, 60_000)
  end

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, :disk)
    data_dir = Keyword.get(opts, :data_dir, TimelessTraces.Config.data_dir())
    base_interval = TimelessTraces.Config.compaction_interval()
    schedule(base_interval)

    {:ok,
     %{
       storage: storage,
       data_dir: data_dir,
       base_interval: base_interval,
       idle_cycles: 0
     }}
  end

  @impl true
  def handle_call(:compact_now, _from, state) do
    result = drain_compact(state)
    update_raw_debt_gauge()
    {:reply, result, %{state | idle_cycles: 0}}
  end

  def handle_call(:merge_now, _from, state) do
    result = maybe_merge_compact(state)
    {:reply, result, %{state | idle_cycles: 0}}
  end

  @impl true
  def handle_info(:compaction_check, state) do
    compact_result = maybe_compact(state)
    merge_result = maybe_merge_compact(state)
    update_raw_debt_gauge()

    cond do
      compact_result == :more ->
        # Raw debt remains — keep compacting continuously, no idle wait.
        schedule(0)
        {:noreply, %{state | idle_cycles: 0}}

      compact_result == :noop and merge_result == :noop ->
        state = %{state | idle_cycles: state.idle_cycles + 1}
        max_backoff = TimelessTraces.Config.compaction_max_backoff()
        next_interval = min(state.base_interval * Bitwise.bsl(1, state.idle_cycles), max_backoff)
        schedule(next_interval)
        {:noreply, state}

      true ->
        schedule(state.base_interval)
        {:noreply, %{state | idle_cycles: 0}}
    end
  end

  defp update_raw_debt_gauge do
    stats = TimelessTraces.Index.raw_block_stats()
    TimelessTraces.IngestPressure.set_raw_debt(stats.total_bytes)
    stats
  end

  # Manual compaction keeps its "compact everything" semantics by looping
  # bounded passes until the backlog is gone.
  defp drain_compact(state) do
    case maybe_compact(state) do
      :more -> drain_compact(state)
      other -> other
    end
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
      do_compact(state, stats)
    else
      :noop
    end
  end

  defp do_compact(state, stats) do
    start_time = System.monotonic_time()
    concurrency = System.schedulers_online()
    output_target = TimelessTraces.Config.merge_compaction_target_size()

    # Bounded pass: one output block per core. Reading the entire raw
    # backlog into memory at once is an OOM hazard when the compactor is
    # behind; the scheduler loops passes back-to-back (:more) instead.
    entry_budget = concurrency * output_target

    {raw_blocks, leftover} =
      take_by_entry_budget(TimelessTraces.Index.raw_block_ids(), entry_budget)

    all_entries =
      Enum.flat_map(raw_blocks, fn {block_id, file_path, _bs, _ec} ->
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

      # Logical size before compression: the raw blocks' on-disk sizes
      # (ETF of the span lists) — no per-entry re-serialization.
      raw_bytes = Enum.reduce(raw_blocks, 0, fn {_bid, _fp, bs, _ec}, acc -> acc + bs end)

      write_target = if state.storage == :memory, do: :memory, else: state.data_dir
      write_opts = compaction_write_opts(stats.total_bytes)
      chunks = Enum.chunk_every(sorted, output_target)

      new_blocks =
        chunks
        |> Task.async_stream(
          fn chunk ->
            case TimelessTraces.Writer.write_block(
                   chunk,
                   write_target,
                   TimelessTraces.Config.compaction_format(),
                   write_opts
                 ) do
              {:ok, meta} -> {meta, chunk}
              _error -> nil
            end
          end,
          max_concurrency: concurrency,
          ordered: false,
          timeout: 120_000
        )
        |> Enum.flat_map(fn
          {:ok, nil} -> []
          {:ok, pair} -> [pair]
        end)

      case new_blocks do
        [] ->
          Logger.warning("TimelessTraces: compaction failed: all chunks errored")
          :noop

        new_blocks ->
          old_ids = Enum.map(raw_blocks, &elem(&1, 0))
          total_bytes = Enum.reduce(new_blocks, 0, fn {meta, _c}, acc -> acc + meta.byte_size end)

          TimelessTraces.Index.compact_blocks_multi(old_ids, new_blocks, {raw_bytes, total_bytes})

          duration = System.monotonic_time() - start_time

          TimelessTraces.Telemetry.event(
            [:timeless_traces, :compaction, :stop],
            %{
              duration: duration,
              raw_blocks: length(raw_blocks),
              entry_count: length(sorted),
              byte_size: total_bytes
            },
            %{}
          )

          if leftover, do: :more, else: :ok
      end
    end
  rescue
    e ->
      Logger.error(
        "TimelessTraces: compaction crashed: #{Exception.format(:error, e, __STACKTRACE__)}"
      )

      :noop
  end

  defp take_by_entry_budget(blocks, budget) do
    {taken_rev, _spent, leftover} =
      Enum.reduce(blocks, {[], 0, false}, fn
        {_bid, _fp, _bs, ec} = block, {taken, spent, false} when spent < budget ->
          {[block | taken], spent + ec, false}

        _block, {taken, spent, _} ->
          {taken, spent, true}
      end)

    {Enum.reverse(taken_rev), leftover}
  end

  # Under heavy raw debt, trade compression ratio for throughput so the
  # backlog drains before ingest backpressure has to engage.
  defp compaction_write_opts(raw_debt_bytes) do
    if raw_debt_bytes > div(TimelessTraces.Config.ingest_raw_debt_limit(), 2) do
      [level: TimelessTraces.Config.compaction_pressure_level()]
    else
      []
    end
  end

  # --- Merge compaction ---

  defp maybe_merge_compact(state) do
    target = TimelessTraces.Config.merge_compaction_target_size()
    min_blocks = TimelessTraces.Config.merge_compaction_min_blocks()
    small_blocks = TimelessTraces.Index.small_compressed_block_ids(target)

    if length(small_blocks) >= min_blocks do
      do_merge_compact(state, small_blocks, target)
    else
      :noop
    end
  end

  defp do_merge_compact(state, small_blocks, target_size) do
    start_time = System.monotonic_time()
    batches = group_into_batches(small_blocks, target_size)

    merged_count =
      Enum.reduce(batches, 0, fn batch, acc ->
        case merge_batch(state, batch) do
          :ok -> acc + 1
          :noop -> acc
        end
      end)

    if merged_count > 0 do
      duration = System.monotonic_time() - start_time

      TimelessTraces.Telemetry.event(
        [:timeless_traces, :merge_compaction, :stop],
        %{
          duration: duration,
          batches_merged: merged_count,
          blocks_consumed: length(small_blocks)
        },
        %{}
      )

      :ok
    else
      :noop
    end
  end

  defp group_into_batches(blocks, target_size) do
    {batches, current} =
      Enum.reduce(blocks, {[], []}, fn {_bid, _fp, _bs, ec} = block, {batches, current} ->
        current_count = Enum.reduce(current, 0, fn {_, _, _, e}, a -> a + e end)

        if current_count + ec > target_size and current != [] do
          {[current | batches], [block]}
        else
          {batches, current ++ [block]}
        end
      end)

    # Only include the last batch if it has >= 2 blocks
    all = if length(current) >= 2, do: [current | batches], else: batches
    Enum.reverse(all)
  end

  defp merge_batch(state, batch) do
    all_entries =
      Enum.flat_map(batch, fn {block_id, file_path, _bs, _ec} ->
        read_result =
          case state.storage do
            :disk -> TimelessTraces.Writer.read_block(file_path, format_from_path(file_path))
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

      # Logical (uncompressed ETF) size for compression stats — one
      # whole-list serialization, not one per span.
      raw_bytes = byte_size(:erlang.term_to_binary(sorted))

      write_target = if state.storage == :memory, do: :memory, else: state.data_dir

      case TimelessTraces.Writer.write_block(
             sorted,
             write_target,
             TimelessTraces.Config.compaction_format()
           ) do
        {:ok, new_meta} ->
          old_ids = Enum.map(batch, &elem(&1, 0))

          TimelessTraces.Index.compact_blocks(
            old_ids,
            new_meta,
            sorted,
            {raw_bytes, new_meta.byte_size}
          )

          :ok

        {:error, reason} ->
          Logger.error("TimelessTraces: merge batch write failed: #{inspect(reason)}")
          :noop
      end
    end
  rescue
    e ->
      Logger.error(
        "TimelessTraces: merge batch crashed: #{Exception.format(:error, e, __STACKTRACE__)}"
      )

      :noop
  end

  defp format_from_path(path) do
    case Path.extname(path) do
      ".ozl" -> :openzl
      ".zst" -> :zstd
      ".raw" -> :raw
      _ -> :raw
    end
  end
end
