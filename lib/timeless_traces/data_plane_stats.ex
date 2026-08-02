defmodule TimelessTraces.DataPlaneStats do
  @moduledoc false

  # Process-lifetime counters for the HTTP/exporter admission boundary. These
  # are intentionally independent of the storage Stats struct: storage stats
  # describe what exists, while these counters describe work moving toward it.
  @key {__MODULE__, :counters}

  @admitted_requests 1
  @admitted_spans 2
  @admitted_bytes 3
  @drained_requests 4
  @completed_spans 5
  @rejected_requests 6
  @failed_spans 7
  @in_flight_batches 8
  @in_flight_spans 9
  @counter_count 9

  @spec install() :: :ok
  def install do
    :persistent_term.put(@key, :atomics.new(@counter_count, []))
    :ok
  end

  @spec admit_request(non_neg_integer()) :: :ok
  def admit_request(body_bytes) do
    add(@admitted_requests, 1)
    add(@admitted_bytes, body_bytes)
  end

  @spec admit_spans(non_neg_integer()) :: :ok
  def admit_spans(count), do: add(@admitted_spans, count)

  @spec reject_request() :: :ok
  def reject_request, do: add(@rejected_requests, 1)

  @spec complete_spans(non_neg_integer()) :: :ok
  def complete_spans(count), do: add(@completed_spans, count)

  @spec fail_spans(non_neg_integer()) :: :ok
  def fail_spans(count), do: add(@failed_spans, count)

  @spec flush_started(non_neg_integer()) :: :ok
  def flush_started(count) do
    add(@in_flight_batches, 1)
    add(@in_flight_spans, count)
  end

  @spec flush_finished(non_neg_integer()) :: :ok
  def flush_finished(count) do
    add(@in_flight_batches, -1)
    add(@in_flight_spans, -count)
  end

  # A flush barrier drains every request admitted before the call. This is
  # deliberately named "drained", not "completed": storage failures are
  # accounted separately because the current product does not retain request
  # identity through its sharded block pipeline.
  @spec mark_requests_drained() :: :ok
  def mark_requests_drained do
    ref = ref()
    :atomics.put(ref, @drained_requests, :atomics.get(ref, @admitted_requests))
    :ok
  end

  @spec snapshot() :: map()
  def snapshot do
    pressure = TimelessTraces.IngestPressure.snapshot()

    %{
      admitted_requests: get(@admitted_requests),
      admitted_spans: get(@admitted_spans),
      admitted_bytes: get(@admitted_bytes),
      drained_requests: get(@drained_requests),
      completed_spans: get(@completed_spans),
      rejected_requests: get(@rejected_requests),
      failed_spans: get(@failed_spans),
      queued_spans: pressure.queued_spans,
      oldest_queue_age_ms: pressure.oldest_queue_age_ms,
      in_flight_batches: get(@in_flight_batches),
      in_flight_spans: get(@in_flight_spans),
      raw_debt_bytes: pressure.raw_debt_bytes,
      overloaded: pressure.overloaded
    }
  end

  defp add(_slot, 0), do: :ok

  defp add(slot, value) do
    :atomics.add(ref(), slot, value)
    :ok
  end

  defp get(slot), do: :atomics.get(ref(), slot)
  defp ref, do: :persistent_term.get(@key)
end
