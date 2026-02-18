defmodule TimelessTraces.WriterTest do
  use ExUnit.Case, async: true

  @data_dir "test/tmp/writer_#{System.unique_integer([:positive])}"

  setup do
    blocks_dir = Path.join(@data_dir, "blocks")
    File.mkdir_p!(blocks_dir)
    on_exit(fn -> File.rm_rf!(@data_dir) end)
    :ok
  end

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

  describe "write_block/3" do
    test "writes a raw block file" do
      spans = make_spans(2)
      assert {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :raw)
      assert meta.entry_count == 2
      assert meta.ts_min == 1_001_000_000
      assert meta.ts_max == 1_002_000_000
      assert meta.byte_size > 0
      assert meta.format == :raw
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".raw")
    end

    test "writes a zstd compressed block file" do
      spans = make_spans(2)
      assert {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :zstd)
      assert meta.format == :zstd
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".zst")
    end

    test "writes an openzl compressed block file" do
      spans = make_spans(2)
      assert {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :openzl)
      assert meta.format == :openzl
      assert meta.entry_count == 2
      assert meta.byte_size > 0
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".ozl")
    end

    test "compressed size is smaller than raw for large blocks" do
      spans = make_spans(200)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :zstd)
      raw_size = byte_size(:erlang.term_to_binary(spans))
      assert meta.byte_size < raw_size
    end

    test "openzl compressed size is smaller than raw for large blocks" do
      spans = make_spans(200)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :openzl)
      raw_size = byte_size(:erlang.term_to_binary(spans))
      assert meta.byte_size < raw_size
    end

    test "writes to memory" do
      spans = make_spans(3)
      assert {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :raw)
      assert meta.file_path == nil
      assert meta.data != nil
      assert meta.entry_count == 3
    end

    test "writes openzl to memory" do
      spans = make_spans(3)
      assert {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      assert meta.file_path == nil
      assert meta.data != nil
      assert meta.entry_count == 3
      assert meta.format == :openzl
    end
  end

  describe "read_block/2" do
    test "roundtrips raw data" do
      spans = make_spans(2)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :raw)
      assert {:ok, read_spans} = TimelessTraces.Writer.read_block(meta.file_path, :raw)
      assert read_spans == spans
    end

    test "roundtrips zstd data" do
      spans = make_spans(2)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :zstd)
      assert {:ok, read_spans} = TimelessTraces.Writer.read_block(meta.file_path, :zstd)
      assert read_spans == spans
    end

    test "roundtrips openzl data" do
      spans = make_spans(2)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, @data_dir, :openzl)
      assert {:ok, read_spans} = TimelessTraces.Writer.read_block(meta.file_path, :openzl)
      assert_spans_equal(spans, read_spans)
    end

    test "returns error for missing file" do
      assert {:error, :enoent} = TimelessTraces.Writer.read_block("/nonexistent/file.zst")
    end

    test "returns error for corrupt zstd file" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.zst"])
      File.write!(corrupt_path, "not valid zstd data")
      assert {:error, :corrupt_block} = TimelessTraces.Writer.read_block(corrupt_path, :zstd)
    end

    test "returns error for corrupt openzl file" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.ozl"])
      File.write!(corrupt_path, "not valid openzl data")
      assert {:error, :corrupt_block} = TimelessTraces.Writer.read_block(corrupt_path, :openzl)
    end
  end

  describe "decompress_block/2" do
    test "decompresses raw block" do
      spans = make_spans(1)
      binary = :erlang.term_to_binary(spans)
      assert {:ok, ^spans} = TimelessTraces.Writer.decompress_block(binary, :raw)
    end

    test "decompresses zstd block" do
      spans = make_spans(1)
      binary = :erlang.term_to_binary(spans)
      compressed = :ezstd.compress(binary)
      assert {:ok, ^spans} = TimelessTraces.Writer.decompress_block(compressed, :zstd)
    end

    test "decompresses openzl block" do
      spans = make_spans(3)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      assert {:ok, read_spans} = TimelessTraces.Writer.decompress_block(meta.data, :openzl)
      assert_spans_equal(spans, read_spans)
    end
  end

  describe "openzl columnar round-trip" do
    test "preserves all span kinds" do
      kinds = [:internal, :server, :client, :producer, :consumer]

      spans =
        Enum.with_index(kinds, fn kind, i ->
          %{
            trace_id: "trace-kind-#{i}",
            span_id: "span-kind-#{i}",
            parent_span_id: nil,
            name: "op-#{kind}",
            kind: kind,
            start_time: 1_000_000_000 + i * 1_000_000,
            end_time: 1_000_000_000 + i * 1_000_000 + 100_000,
            duration_ns: 100_000,
            status: :ok,
            status_message: nil,
            attributes: %{},
            events: [],
            resource: %{},
            instrumentation_scope: nil
          }
        end)

      {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      {:ok, read_spans} = TimelessTraces.Writer.decompress_block(meta.data, :openzl)

      for {orig, rec} <- Enum.zip(spans, read_spans) do
        assert orig.kind == rec.kind
      end
    end

    test "preserves all status values" do
      statuses = [:ok, :error, :unset]

      spans =
        Enum.with_index(statuses, fn status, i ->
          %{
            trace_id: "trace-status-#{i}",
            span_id: "span-status-#{i}",
            parent_span_id: nil,
            name: "op",
            kind: :internal,
            start_time: 1_000_000_000 + i * 1_000_000,
            end_time: 1_000_000_000 + i * 1_000_000 + 100_000,
            duration_ns: 100_000,
            status: status,
            status_message: if(status == :error, do: "something broke", else: nil),
            attributes: %{},
            events: [],
            resource: %{},
            instrumentation_scope: nil
          }
        end)

      {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      {:ok, read_spans} = TimelessTraces.Writer.decompress_block(meta.data, :openzl)

      for {orig, rec} <- Enum.zip(spans, read_spans) do
        assert orig.status == rec.status
        assert orig.status_message == rec.status_message
      end
    end

    test "preserves nil vs present optional fields" do
      spans = [
        %{
          trace_id: "t1",
          span_id: "s1",
          parent_span_id: "parent-1",
          name: "with-parent",
          kind: :server,
          start_time: 1_000_000_000,
          end_time: 1_000_500_000,
          duration_ns: 500_000,
          status: :error,
          status_message: "failed",
          attributes: %{"key" => "value"},
          events: [%{name: "exception", timestamp: 1_000_100_000}],
          resource: %{"service.name" => "test"},
          instrumentation_scope: %{name: "my_lib", version: "1.0"}
        },
        %{
          trace_id: "t2",
          span_id: "s2",
          parent_span_id: nil,
          name: "root-span",
          kind: :internal,
          start_time: 1_001_000_000,
          end_time: 1_001_500_000,
          duration_ns: 500_000,
          status: :ok,
          status_message: nil,
          attributes: %{},
          events: [],
          resource: %{},
          instrumentation_scope: nil
        }
      ]

      {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      {:ok, read_spans} = TimelessTraces.Writer.decompress_block(meta.data, :openzl)

      [r1, r2] = read_spans
      assert r1.parent_span_id == "parent-1"
      assert r1.status_message == "failed"
      assert r1.attributes == %{"key" => "value"}
      assert r1.events == [%{name: "exception", timestamp: 1_000_100_000}]
      assert r1.instrumentation_scope == %{name: "my_lib", version: "1.0"}

      assert r2.parent_span_id == nil
      assert r2.status_message == nil
      assert r2.instrumentation_scope == nil
    end

    test "preserves large blocks with many spans" do
      spans = make_spans(500)
      {:ok, meta} = TimelessTraces.Writer.write_block(spans, :memory, :openzl)
      {:ok, read_spans} = TimelessTraces.Writer.decompress_block(meta.data, :openzl)
      assert length(read_spans) == 500
      assert_spans_equal(spans, read_spans)
    end
  end

  # Compares span maps field-by-field (openzl round-trip produces plain maps)
  defp assert_spans_equal(originals, recovered) do
    assert length(originals) == length(recovered)

    for {orig, rec} <- Enum.zip(originals, recovered) do
      assert orig.trace_id == rec.trace_id
      assert orig.span_id == rec.span_id
      assert orig.parent_span_id == rec.parent_span_id
      assert orig.name == rec.name
      assert orig.kind == rec.kind
      assert orig.start_time == rec.start_time
      assert orig.end_time == rec.end_time
      assert orig.duration_ns == rec.duration_ns
      assert orig.status == rec.status
      assert orig.status_message == rec.status_message
      assert orig.attributes == rec.attributes
      assert orig.events == rec.events
      assert orig.resource == rec.resource
      assert orig.instrumentation_scope == rec.instrumentation_scope
    end
  end
end
