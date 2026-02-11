defmodule SpanStream.WriterTest do
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
      assert {:ok, meta} = SpanStream.Writer.write_block(spans, @data_dir, :raw)
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
      assert {:ok, meta} = SpanStream.Writer.write_block(spans, @data_dir, :zstd)
      assert meta.format == :zstd
      assert File.exists?(meta.file_path)
      assert String.ends_with?(meta.file_path, ".zst")
    end

    test "compressed size is smaller than raw for large blocks" do
      spans = make_spans(200)
      {:ok, meta} = SpanStream.Writer.write_block(spans, @data_dir, :zstd)
      raw_size = byte_size(:erlang.term_to_binary(spans))
      assert meta.byte_size < raw_size
    end

    test "writes to memory" do
      spans = make_spans(3)
      assert {:ok, meta} = SpanStream.Writer.write_block(spans, :memory, :raw)
      assert meta.file_path == nil
      assert meta.data != nil
      assert meta.entry_count == 3
    end
  end

  describe "read_block/2" do
    test "roundtrips raw data" do
      spans = make_spans(2)
      {:ok, meta} = SpanStream.Writer.write_block(spans, @data_dir, :raw)
      assert {:ok, read_spans} = SpanStream.Writer.read_block(meta.file_path, :raw)
      assert read_spans == spans
    end

    test "roundtrips zstd data" do
      spans = make_spans(2)
      {:ok, meta} = SpanStream.Writer.write_block(spans, @data_dir, :zstd)
      assert {:ok, read_spans} = SpanStream.Writer.read_block(meta.file_path, :zstd)
      assert read_spans == spans
    end

    test "returns error for missing file" do
      assert {:error, :enoent} = SpanStream.Writer.read_block("/nonexistent/file.zst")
    end

    test "returns error for corrupt zstd file" do
      corrupt_path = Path.join([@data_dir, "blocks", "corrupt.zst"])
      File.write!(corrupt_path, "not valid zstd data")
      assert {:error, :corrupt_block} = SpanStream.Writer.read_block(corrupt_path, :zstd)
    end
  end

  describe "decompress_block/2" do
    test "decompresses raw block" do
      spans = make_spans(1)
      binary = :erlang.term_to_binary(spans)
      assert {:ok, ^spans} = SpanStream.Writer.decompress_block(binary, :raw)
    end

    test "decompresses zstd block" do
      spans = make_spans(1)
      binary = :erlang.term_to_binary(spans)
      compressed = :ezstd.compress(binary)
      assert {:ok, ^spans} = SpanStream.Writer.decompress_block(compressed, :zstd)
    end
  end
end
