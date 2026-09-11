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
        schedule(1)
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
    concurrency = TimelessTraces.Config.query_concurrency()
    output_target = TimelessTraces.Config.merge_compaction_target_size()

    # Bounded pass: one output block per core. Reading the entire raw
    # backlog into memory at once is an OOM hazard when the compactor is
    # behind; the scheduler loops passes back-to-back (:more) instead.
    entry_budget = concurrency * output_target

    {raw_blocks, leftover} =
      take_by_entry_budget(TimelessTraces.Index.raw_block_ids(), entry_budget)

    all_entries = read_blocks(raw_blocks, state, :raw, concurrency)

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
    take_by_entry_budget(blocks, budget, 0, [])
  end

  defp take_by_entry_budget([], _budget, _spent, taken), do: {Enum.reverse(taken), false}

  defp take_by_entry_budget(_blocks, budget, spent, taken) when spent >= budget,
    do: {Enum.reverse(taken), true}

  defp take_by_entry_budget([block | rest], budget, spent, taken) do
    {_bid, _fp, _bs, entries} = block
    take_by_entry_budget(rest, budget, spent + entries, [block | taken])
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
    group_into_batches(blocks, target_size, [], [], 0, 0)
  end

  defp group_into_batches([], _target, batches, current, _entries, current_length) do
    batches = if current_length >= 2, do: [Enum.reverse(current) | batches], else: batches
    Enum.reverse(batches)
  end

  defp group_into_batches(
         [{_bid, _fp, _bs, entries} = block | rest],
         target,
         batches,
         current,
         current_entries,
         current_length
       ) do
    if current != [] and current_entries + entries > target do
      group_into_batches(rest, target, [Enum.reverse(current) | batches], [block], entries, 1)
    else
      group_into_batches(
        rest,
        target,
        batches,
        [block | current],
        current_entries + entries,
        current_length + 1
      )
    end
  end

  defp merge_batch(state, batch) do
    all_entries = read_blocks(batch, state, :from_path, TimelessTraces.Config.query_concurrency())

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

  defp read_blocks(blocks, state, format, concurrency) do
    blocks
    |> Task.async_stream(
      fn {block_id, file_path, _bytes, _entries} ->
        read_result =
          case state.storage do
            :disk ->
              block_format =
                if format == :from_path, do: format_from_path(file_path), else: format

              TimelessTraces.Writer.read_block(file_path, block_format)

            :memory ->
              TimelessTraces.Index.read_block_data(block_id)
          end

        case read_result do
          {:ok, entries} -> entries
          {:error, _} -> []
        end
      end,
      ordered: false,
      max_concurrency: max(concurrency, 1),
      timeout: 120_000
    )
    |> Enum.flat_map(fn
      {:ok, entries} -> entries
      {:exit, _reason} -> []
    end)
  end
end
