defmodule TimelessTraces.TelemetryTest do
  use ExUnit.Case, async: true

  test "span preserves results with and without handlers" do
    event = [:timeless_traces, :test, :stop]
    result = {%{value: 1}, %{source: :test}}

    assert TimelessTraces.Telemetry.span([:timeless_traces, :test], %{}, fn -> result end) ==
             result

    test_pid = self()

    handler_id = "timeless-traces-telemetry-test-#{System.unique_integer()}"

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        &__MODULE__.handle_event/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert TimelessTraces.Telemetry.span([:timeless_traces, :test], %{base: true}, fn ->
             result
           end) ==
             result

    assert_receive {:event, %{value: 1, duration: duration}, %{base: true, source: :test}}
    assert is_integer(duration)
  end

  def handle_event(_event, measurements, metadata, test_pid) do
    send(test_pid, {:event, measurements, metadata})
  end
end
