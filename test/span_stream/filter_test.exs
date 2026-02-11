defmodule SpanStream.FilterTest do
  use ExUnit.Case, async: true

  alias SpanStream.Filter

  defp make_span(overrides \\ %{}) do
    Map.merge(
      %{
        trace_id: "trace-1",
        span_id: "span-1",
        parent_span_id: nil,
        name: "HTTP GET /users",
        kind: :server,
        start_time: 1_000_000_000,
        end_time: 1_050_000_000,
        duration_ns: 50_000_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET", "service.name" => "api"},
        events: [],
        resource: %{"service.name" => "api"},
        instrumentation_scope: nil
      },
      overrides
    )
  end

  describe "matches?/2" do
    test "matches by name substring" do
      span = make_span()
      assert Filter.matches?(span, name: "GET")
      assert Filter.matches?(span, name: "users")
      refute Filter.matches?(span, name: "POST")
    end

    test "matches by kind" do
      span = make_span()
      assert Filter.matches?(span, kind: :server)
      refute Filter.matches?(span, kind: :client)
    end

    test "matches by status" do
      span = make_span()
      assert Filter.matches?(span, status: :ok)
      refute Filter.matches?(span, status: :error)
    end

    test "matches by service" do
      span = make_span()
      assert Filter.matches?(span, service: "api")
      refute Filter.matches?(span, service: "web")
    end

    test "matches by min_duration" do
      span = make_span(%{duration_ns: 100_000_000})
      assert Filter.matches?(span, min_duration: 50_000_000)
      refute Filter.matches?(span, min_duration: 200_000_000)
    end

    test "matches by max_duration" do
      span = make_span(%{duration_ns: 100_000_000})
      assert Filter.matches?(span, max_duration: 200_000_000)
      refute Filter.matches?(span, max_duration: 50_000_000)
    end

    test "matches by trace_id" do
      span = make_span()
      assert Filter.matches?(span, trace_id: "trace-1")
      refute Filter.matches?(span, trace_id: "trace-2")
    end

    test "matches by since/until" do
      span = make_span(%{start_time: 1_000_000_000})
      assert Filter.matches?(span, since: 999_000_000)
      refute Filter.matches?(span, since: 2_000_000_000)
      assert Filter.matches?(span, until: 2_000_000_000)
      refute Filter.matches?(span, until: 999_000_000)
    end

    test "matches by attributes" do
      span = make_span()
      assert Filter.matches?(span, attributes: %{"http.method" => "GET"})
      refute Filter.matches?(span, attributes: %{"http.method" => "POST"})
    end

    test "all filters combined" do
      span = make_span()
      assert Filter.matches?(span, name: "GET", kind: :server, status: :ok)
      refute Filter.matches?(span, name: "GET", kind: :client, status: :ok)
    end

    test "empty filters matches everything" do
      span = make_span()
      assert Filter.matches?(span, [])
    end
  end

  describe "filter/2" do
    test "filters list of spans" do
      spans = [
        make_span(%{status: :ok, name: "good"}),
        make_span(%{status: :error, name: "bad"}),
        make_span(%{status: :ok, name: "also good"})
      ]

      assert length(Filter.filter(spans, status: :error)) == 1
      assert length(Filter.filter(spans, status: :ok)) == 2
    end
  end
end
