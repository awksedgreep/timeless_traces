defmodule TimelessTraces.IngestPressure do
  @moduledoc false

  # Shared gauges for ingest backpressure, readable without touching any
  # GenServer: one slot per buffer shard holding its queued entry count
  # (buffer + pending batches + in-flight flushes), plus one slot for the
  # compactor's raw-block debt in bytes.
  #
  # Below the watermark, ingest is a cast (bursts absorbed at full speed).
  # At or above it — or when raw debt is past its limit — batch ingest
  # switches to a call, so producers pace-match the durable drain rate
  # instead of converting overload into unbounded memory. Nothing is
  # dropped or refused.

  @key {__MODULE__, :gauges}

  @spec install(pos_integer()) :: :ok
  def install(shard_count) do
    # One queued-span counter and one oldest-enqueue timestamp per shard,
    # followed by the compactor's raw-debt gauge.
    ref = :atomics.new(shard_count * 2 + 1, [])
    :persistent_term.put(@key, {ref, shard_count})
    :ok
  end

  # The gauge is a producer-side counter, incremented before the message
  # is sent and decremented when entries reach disk (or are dropped).
  # Counting at the send point is what bounds the shard MAILBOX — a
  # server-side mirror of processed-but-unflushed entries misses casts
  # queued in the mailbox, which is exactly where overload accumulates.
  @spec add(non_neg_integer(), pos_integer()) :: :ok
  def add(shard, n) do
    {ref, count} = :persistent_term.get(@key)
    new_value = :atomics.add_get(ref, shard + 1, n)

    if new_value == n do
      :atomics.put(ref, oldest_slot(count, shard), System.system_time(:millisecond))
    end

    :ok
  end

  @spec sub(non_neg_integer(), pos_integer()) :: :ok
  def sub(shard, n) do
    {ref, count} = :persistent_term.get(@key)
    new_value = :atomics.sub_get(ref, shard + 1, n)

    if new_value <= 0 do
      :atomics.put(ref, shard + 1, 0)
      :atomics.put(ref, oldest_slot(count, shard), 0)
    end

    if new_value < TimelessTraces.Config.ingest_soft_watermark(), do: notify_capacity(shard)

    :ok
  end

  @spec reset(non_neg_integer()) :: :ok
  def reset(shard) do
    {ref, count} = :persistent_term.get(@key)
    :atomics.put(ref, shard + 1, 0)
    :atomics.put(ref, oldest_slot(count, shard), 0)
    :ok
  end

  @spec queued(non_neg_integer()) :: non_neg_integer()
  def queued(shard) do
    {ref, _count} = :persistent_term.get(@key)
    :atomics.get(ref, shard + 1)
  end

  @spec set_raw_debt(non_neg_integer()) :: :ok
  def set_raw_debt(bytes) do
    {ref, count} = :persistent_term.get(@key)
    :atomics.put(ref, raw_debt_slot(count), bytes)

    if bytes < TimelessTraces.Config.ingest_raw_debt_limit() do
      Enum.each(0..(count - 1), &notify_capacity/1)
    end

    :ok
  end

  @spec raw_debt() :: non_neg_integer()
  def raw_debt do
    {ref, count} = :persistent_term.get(@key)
    :atomics.get(ref, raw_debt_slot(count))
  end

  @spec overloaded?(non_neg_integer()) :: boolean()
  def overloaded?(shard) do
    queued(shard) >= TimelessTraces.Config.ingest_soft_watermark() or
      raw_debt() >= TimelessTraces.Config.ingest_raw_debt_limit()
  end

  @spec any_overloaded?() :: boolean()
  def any_overloaded? do
    {_ref, count} = :persistent_term.get(@key)
    Enum.any?(0..(count - 1), &overloaded?/1)
  end

  @spec await_capacity(non_neg_integer(), non_neg_integer()) :: :ok | :timeout
  def await_capacity(shard, timeout) do
    if overloaded?(shard) do
      key = {:ingest_capacity, shard}

      case Registry.register(TimelessTraces.Registry, key, nil) do
        {:ok, _owner} ->
          deadline = System.monotonic_time(:millisecond) + timeout

          try do
            await_capacity_message(shard, deadline)
          after
            Registry.unregister(TimelessTraces.Registry, key)
          end

        {:error, _reason} ->
          :timeout
      end
    else
      :ok
    end
  end

  @spec snapshot() :: %{
          queued_spans: non_neg_integer(),
          oldest_queue_age_ms: non_neg_integer(),
          raw_debt_bytes: non_neg_integer(),
          overloaded: boolean()
        }
  def snapshot do
    {ref, count} = :persistent_term.get(@key)
    now_ms = System.system_time(:millisecond)

    {queued_spans, oldest_ms} =
      Enum.reduce(0..(count - 1), {0, nil}, fn shard, {queued_acc, oldest_acc} ->
        queued = :atomics.get(ref, shard + 1)
        enqueued_at = :atomics.get(ref, oldest_slot(count, shard))

        oldest =
          if queued > 0 and enqueued_at > 0 do
            if oldest_acc == nil, do: enqueued_at, else: min(oldest_acc, enqueued_at)
          else
            oldest_acc
          end

        {queued_acc + queued, oldest}
      end)

    %{
      queued_spans: queued_spans,
      oldest_queue_age_ms: if(oldest_ms, do: max(now_ms - oldest_ms, 0), else: 0),
      raw_debt_bytes: raw_debt(),
      overloaded: any_overloaded?()
    }
  end

  defp oldest_slot(count, shard), do: count + shard + 1
  defp raw_debt_slot(count), do: count * 2 + 1

  defp await_capacity_message(shard, deadline) do
    if overloaded?(shard) do
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        :timeout
      else
        receive do
          {:timeless_traces, :capacity_available, ^shard} ->
            await_capacity_message(shard, deadline)
        after
          remaining -> :timeout
        end
      end
    else
      :ok
    end
  end

  defp notify_capacity(shard) do
    if Process.whereis(TimelessTraces.Registry) do
      Registry.dispatch(TimelessTraces.Registry, {:ingest_capacity, shard}, fn waiters ->
        Enum.each(waiters, fn {pid, _value} ->
          send(pid, {:timeless_traces, :capacity_available, shard})
        end)
      end)
    end

    :ok
  end
end
