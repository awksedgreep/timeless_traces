defmodule TimelessTraces.SpanTest do
  use ExUnit.Case, async: true

  alias TimelessTraces.Span

  describe "from_map/1" do
    test "creates span from atom-keyed map" do
      span =
        Span.from_map(%{
          trace_id: "abc123",
          span_id: "def456",
          parent_span_id: nil,
          name: "HTTP GET",
          kind: :server,
          start_time: 1_000_000_000,
          end_time: 1_500_000_000,
          status: :ok,
          attributes: %{"http.method" => "GET"},
          events: [],
          resource: %{"service.name" => "my_app"}
        })

      assert %Span{} = span
      assert span.trace_id == "abc123"
      assert span.span_id == "def456"
      assert span.parent_span_id == nil
      assert span.name == "HTTP GET"
      assert span.kind == :server
      assert span.start_time == 1_000_000_000
      assert span.end_time == 1_500_000_000
      assert span.duration_ns == 500_000_000
      assert span.status == :ok
      assert span.attributes == %{"http.method" => "GET"}
      assert span.resource == %{"service.name" => "my_app"}
    end

    test "computes duration_ns automatically" do
      span =
        Span.from_map(%{
          trace_id: "a",
          span_id: "b",
          name: "test",
          start_time: 100,
          end_time: 350
        })

      assert span.duration_ns == 250
    end

    test "defaults for missing fields" do
      span =
        Span.from_map(%{trace_id: "a", span_id: "b", name: "test", start_time: 0, end_time: 0})

      assert span.kind == :internal
      assert span.status == :unset
      assert span.attributes == %{}
      assert span.events == []
      assert span.resource == %{}
    end
  end

  describe "encode_trace_id/1" do
    test "encodes integer to 32-char hex" do
      assert Span.encode_trace_id(255) == "000000000000000000000000000000ff"
      assert String.length(Span.encode_trace_id(255)) == 32
    end

    test "passes through binary" do
      assert Span.encode_trace_id("abc123") == "abc123"
    end
  end

  describe "encode_span_id/1" do
    test "encodes integer to 16-char hex" do
      assert Span.encode_span_id(255) == "00000000000000ff"
      assert String.length(Span.encode_span_id(255)) == 16
    end

    test "passes through binary" do
      assert Span.encode_span_id("abc") == "abc"
    end
  end

  describe "normalize_kind/1" do
    test "normalizes OTel kind atoms" do
      assert Span.normalize_kind(:SPAN_KIND_SERVER) == :server
      assert Span.normalize_kind(:SPAN_KIND_CLIENT) == :client
      assert Span.normalize_kind(:SPAN_KIND_INTERNAL) == :internal
      assert Span.normalize_kind(:SPAN_KIND_PRODUCER) == :producer
      assert Span.normalize_kind(:SPAN_KIND_CONSUMER) == :consumer
    end

    test "passes through standard atoms" do
      assert Span.normalize_kind(:server) == :server
      assert Span.normalize_kind(:client) == :client
    end

    test "unknown defaults to internal" do
      assert Span.normalize_kind(:unknown) == :internal
    end
  end

  describe "normalize_status/1" do
    test "handles standard status atoms" do
      assert Span.normalize_status(:ok) == :ok
      assert Span.normalize_status(:error) == :error
      assert Span.normalize_status(:unset) == :unset
    end

    test "handles string statuses" do
      assert Span.normalize_status("ok") == :ok
      assert Span.normalize_status("error") == :error
    end

    test "unknown defaults to unset" do
      assert Span.normalize_status(:something) == :unset
    end
  end

  describe "normalize_attributes/1" do
    test "normalizes map with atom keys" do
      result = Span.normalize_attributes(%{method: "GET", code: 200})
      assert result == %{"method" => "GET", "code" => 200}
    end

    test "normalizes keyword list" do
      result = Span.normalize_attributes([{:method, "GET"}])
      assert result == %{"method" => "GET"}
    end

    test "handles non-collection input" do
      assert Span.normalize_attributes(nil) == %{}
    end
  end
end
