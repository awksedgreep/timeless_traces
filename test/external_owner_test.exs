defmodule TimelessTraces.ExternalOwnerTest do
  use ExUnit.Case, async: true

  test "external ownership starts no storage, buffer, exporter, or Rocket child" do
    assert [] = TimelessTraces.Application.configured_children(:external)
  end

  test "unknown ownership fails explicitly" do
    assert_raise ArgumentError, ~r/expected :embedded or :external/, fn ->
      TimelessTraces.Application.configured_children(:automatic)
    end
  end
end
