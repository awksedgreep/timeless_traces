defmodule TimelessTraces.BufferShardTest do
  use ExUnit.Case, async: true

  alias TimelessTraces.BufferShard

  test "names use the process registry instead of dynamically-created atoms" do
    assert BufferShard.name(123_456) ==
             {:via, Registry, {TimelessTraces.ProcessRegistry, {:buffer, 123_456}}}
  end

  test "routing reuses a batch's shard count and preserves trace locality" do
    first = %{trace_id: "same-trace", start_time: 1, name: "first"}
    second = %{trace_id: "same-trace", start_time: 2, name: "second"}

    assert BufferShard.shard_for(first, 8) == BufferShard.shard_for(second, 8)
    assert BufferShard.shard_for(first, 8) in 0..7
  end
end
