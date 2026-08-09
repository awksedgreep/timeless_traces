defmodule TimelessTraces.OpenzlFormatClassificationTest do
  @moduledoc """
  A block written by an OpenZL version this build cannot decode must be
  reported as an unreadable format, not as corruption.

  The distinction drives opposite operator responses: corruption means the
  bytes are gone and you restore from backup, an unreadable format means the
  bytes are intact and you need a different decoder. Reporting the first when
  the second is true sends recovery in the wrong direction.

  See docs/2026-08-09_legacy_migration_diagnostics.md.
  """

  use ExUnit.Case, async: true

  alias TimelessTraces.Writer

  # A real block written in production by ex_openzl 0.4.6 (OpenZL 0.1.x).
  # It cannot be regenerated here: producing one requires the old encoder, which
  # is precisely the version this build can no longer read.
  @legacy_fixture Path.join([__DIR__, "..", "fixtures", "legacy_openzl_0_1_x_block.ozl"])

  defp make_spans(count) do
    for i <- 1..count do
      %{
        trace_id: "trace-#{i}",
        span_id: "span-#{i}",
        parent_span_id: nil,
        name: "operation #{i}",
        kind: :server,
        start_time: 1_000_000_000 + i * 1_000_000,
        end_time: 1_000_000_000 + i * 1_000_000 + 500_000,
        duration_ns: 500_000,
        status: :ok,
        status_message: nil,
        attributes: %{"http.method" => "GET"},
        events: [],
        resource: %{"service.name" => "test"},
        instrumentation_scope: nil
      }
    end
  end

  defp fresh_openzl_block do
    {:ok, meta} = Writer.write_block(make_spans(40), :memory, :openzl)
    meta.data
  end

  describe "a frame from an unreadable OpenZL version" do
    test "is reported as :incompatible_format, not :corrupt_block" do
      legacy = File.read!(@legacy_fixture)

      assert {:error, :incompatible_format} = Writer.decompress_block(legacy, :openzl)
    end

    test "is structurally intact, which is why it is not corruption" do
      legacy = File.read!(@legacy_fixture)

      # The container still parses. Only the compression graph inside it is
      # unreadable, so the stored bytes are recoverable by a suitable decoder.
      assert {:ok, info} = ExOpenzl.frame_info(legacy)
      assert is_integer(info[:format_version])
    end
  end

  describe "genuinely damaged data stays :corrupt_block" do
    # These are the regression guards. An earlier attempt at this fix keyed only
    # on "does the frame still introspect?", which reported damaged payloads as
    # a version problem — a worse answer than the bug it replaced, because it
    # tells an operator their lost data is fine.

    test "payload corruption with an intact header" do
      fresh = fresh_openzl_block()
      mid = div(byte_size(fresh), 2)
      <<head::binary-size(^mid), _::binary-size(64), tail::binary>> = fresh
      mangled = head <> :binary.copy(<<0xFF>>, 64) <> tail

      # The header still parses, so introspection alone cannot save us here.
      assert {:ok, _} = ExOpenzl.frame_info(mangled)
      assert {:error, :corrupt_block} = Writer.decompress_block(mangled, :openzl)
    end

    test "truncated frame" do
      fresh = fresh_openzl_block()
      truncated = binary_part(fresh, 0, div(byte_size(fresh), 3))

      assert {:ok, _} = ExOpenzl.frame_info(truncated)
      assert {:error, :corrupt_block} = Writer.decompress_block(truncated, :openzl)
    end

    test "data that is not an OpenZL frame at all" do
      assert {:error, :corrupt_block} =
               Writer.decompress_block("not valid openzl data", :openzl)
    end
  end

  describe "valid blocks are unaffected" do
    test "a block written by this build still round-trips" do
      assert {:ok, spans} = Writer.decompress_block(fresh_openzl_block(), :openzl)
      assert length(spans) == 40
    end
  end
end
