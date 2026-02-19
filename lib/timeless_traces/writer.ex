defmodule TimelessTraces.Writer do
  @moduledoc false

  require Logger

  @type block_meta :: %{
          block_id: integer(),
          file_path: String.t() | nil,
          byte_size: non_neg_integer(),
          entry_count: non_neg_integer(),
          ts_min: integer(),
          ts_max: integer(),
          format: :raw | :zstd | :openzl
        }

  @spec write_block([map()], String.t() | :memory, :raw | :zstd | :openzl, keyword()) ::
          {:ok, block_meta()} | {:error, term()}
  def write_block(entries, target, format \\ :raw, opts \\ [])

  def write_block(entries, :memory, format, opts) do
    data =
      case format do
        :raw ->
          :erlang.term_to_binary(entries)

        :zstd ->
          binary = :erlang.term_to_binary(entries)

          :ezstd.compress(
            binary,
            Keyword.get(opts, :level, TimelessTraces.Config.compression_level())
          )

        :openzl ->
          columns = columnar_serialize(entries)
          {:ok, ctx} = ExOpenzl.create_compression_context()
          level = Keyword.get(opts, :level, TimelessTraces.Config.compression_level())
          ExOpenzl.set_compression_level(ctx, level)
          {:ok, compressed} = ExOpenzl.compress_multi_typed(ctx, columns)
          compressed
      end

    block_id = System.unique_integer([:positive, :monotonic])
    {ts_min, ts_max, count} = ts_min_max_count(entries)

    meta = %{
      block_id: block_id,
      file_path: nil,
      byte_size: byte_size(data),
      entry_count: count,
      ts_min: ts_min,
      ts_max: ts_max,
      data: data,
      format: format
    }

    {:ok, meta}
  end

  def write_block(entries, data_dir, format, opts) do
    {data, ext} =
      case format do
        :raw ->
          {:erlang.term_to_binary(entries), "raw"}

        :zstd ->
          binary = :erlang.term_to_binary(entries)

          {:ezstd.compress(
             binary,
             Keyword.get(opts, :level, TimelessTraces.Config.compression_level())
           ), "zst"}

        :openzl ->
          columns = columnar_serialize(entries)
          {:ok, ctx} = ExOpenzl.create_compression_context()
          level = Keyword.get(opts, :level, TimelessTraces.Config.compression_level())
          ExOpenzl.set_compression_level(ctx, level)
          {:ok, compressed} = ExOpenzl.compress_multi_typed(ctx, columns)
          {compressed, "ozl"}
      end

    block_id = System.unique_integer([:positive, :monotonic])
    filename = "#{String.pad_leading(Integer.to_string(block_id), 12, "0")}.#{ext}"
    file_path = Path.join([data_dir, "blocks", filename])

    case File.write(file_path, data) do
      :ok ->
        {ts_min, ts_max, count} = ts_min_max_count(entries)

        meta = %{
          block_id: block_id,
          file_path: file_path,
          byte_size: byte_size(data),
          entry_count: count,
          ts_min: ts_min,
          ts_max: ts_max,
          format: format
        }

        {:ok, meta}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ts_min_max_count([first | rest]) do
    ts = first.start_time

    Enum.reduce(rest, {ts, ts, 1}, fn entry, {mn, mx, c} ->
      t = entry.start_time
      {min(t, mn), max(t, mx), c + 1}
    end)
  end

  @spec decompress_block(binary(), :raw | :zstd | :openzl) ::
          {:ok, [map()]} | {:error, :corrupt_block}
  def decompress_block(data, format \\ :zstd)

  def decompress_block(data, :raw) do
    try do
      {:ok, :erlang.binary_to_term(data)}
    rescue
      e ->
        Logger.warning("TimelessTraces: corrupt raw block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  def decompress_block(compressed, :zstd) do
    try do
      binary = :ezstd.decompress(compressed)
      {:ok, :erlang.binary_to_term(binary)}
    rescue
      e ->
        Logger.warning("TimelessTraces: corrupt block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  def decompress_block(compressed, :openzl) do
    try do
      {:ok, ctx} = ExOpenzl.create_decompression_context()
      {:ok, outputs} = ExOpenzl.decompress_multi_typed(ctx, compressed)
      {:ok, columnar_deserialize(outputs)}
    rescue
      e ->
        Logger.warning("TimelessTraces: corrupt openzl block data: #{inspect(e)}")
        {:error, :corrupt_block}
    end
  end

  @spec read_block(String.t(), :raw | :zstd | :openzl) :: {:ok, [map()]} | {:error, term()}
  def read_block(file_path, format \\ :zstd) do
    case File.read(file_path) do
      {:ok, data} ->
        decompress_block(data, format)

      {:error, reason} ->
        Logger.warning("TimelessTraces: cannot read block #{file_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # --- Columnar serialization for OpenZL ---

  @kind_to_u8 %{internal: 0, server: 1, client: 2, producer: 3, consumer: 4}
  @u8_to_kind %{0 => :internal, 1 => :server, 2 => :client, 3 => :producer, 4 => :consumer}

  @status_to_u8 %{ok: 0, error: 1, unset: 2}
  @u8_to_status %{0 => :ok, 1 => :error, 2 => :unset}

  defp columnar_serialize(entries) do
    {start_times, end_times, durations, kinds, statuses, trace_ids, span_ids, parent_span_ids,
     names, status_messages, rest_tuples} =
      Enum.reduce(
        entries,
        {[], [], [], [], [], [], [], [], [], [], []},
        fn entry, {st, et, dur, ki, sta, tid, sid, psid, nm, sm, rt} ->
          start_time = entry.start_time
          end_time = entry.end_time

          duration_ns =
            Map.get(entry, :duration_ns) ||
              if start_time && end_time, do: end_time - start_time, else: 0

          kind_u8 = Map.get(@kind_to_u8, entry.kind, 0)
          status_u8 = Map.get(@status_to_u8, entry.status, 2)

          rest_tuple =
            {Map.get(entry, :attributes, %{}), Map.get(entry, :events, []),
             Map.get(entry, :resource, %{}), Map.get(entry, :instrumentation_scope)}

          {
            [start_time | st],
            [end_time | et],
            [duration_ns | dur],
            [kind_u8 | ki],
            [status_u8 | sta],
            [to_string(entry.trace_id) | tid],
            [to_string(entry.span_id) | sid],
            [to_string(entry.parent_span_id || "") | psid],
            [to_string(entry.name) | nm],
            [to_string(entry.status_message || "") | sm],
            [rest_tuple | rt]
          }
        end
      )

    # Reverse to restore original order
    start_times = Enum.reverse(start_times)
    end_times = Enum.reverse(end_times)
    durations = Enum.reverse(durations)
    kinds = Enum.reverse(kinds)
    statuses = Enum.reverse(statuses)
    trace_ids = Enum.reverse(trace_ids)
    span_ids = Enum.reverse(span_ids)
    parent_span_ids = Enum.reverse(parent_span_ids)
    names = Enum.reverse(names)
    status_messages = Enum.reverse(status_messages)

    # Batch all rest tuples into a single term_to_binary call for atom sharing
    rest_bin = :erlang.term_to_binary(Enum.reverse(rest_tuples))
    rest_lengths = <<byte_size(rest_bin)::native-unsigned-32>>

    [
      {:numeric, pack_u64s(start_times), 8},
      {:numeric, pack_u64s(end_times), 8},
      {:numeric, pack_u64s(durations), 8},
      {:numeric, pack_u8s(kinds), 1},
      {:numeric, pack_u8s(statuses), 1},
      pack_string_column(trace_ids),
      pack_string_column(span_ids),
      pack_string_column(parent_span_ids),
      pack_string_column(names),
      pack_string_column(status_messages),
      {:string, rest_bin, rest_lengths}
    ]
  end

  defp pack_u64s(ints) do
    IO.iodata_to_binary(for i <- ints, do: <<i::little-unsigned-64>>)
  end

  defp pack_u8s(ints) do
    IO.iodata_to_binary(for i <- ints, do: <<i::unsigned-8>>)
  end

  defp pack_string_column(strings) do
    data = IO.iodata_to_binary(strings)
    lengths = IO.iodata_to_binary(for s <- strings, do: <<byte_size(s)::native-unsigned-32>>)
    {:string, data, lengths}
  end

  defp columnar_deserialize(outputs) do
    [
      start_time_out,
      end_time_out,
      duration_out,
      kind_out,
      status_out,
      trace_id_out,
      span_id_out,
      parent_span_id_out,
      name_out,
      status_message_out,
      rest_blob_out
    ] = outputs

    n = start_time_out.num_elements

    start_times = unpack_u64s(start_time_out.data, n)
    end_times = unpack_u64s(end_time_out.data, n)
    durations = unpack_u64s(duration_out.data, n)
    kinds = unpack_u8s(kind_out.data, n)
    statuses = unpack_u8s(status_out.data, n)

    trace_ids = split_strings(trace_id_out.data, trace_id_out.string_lengths)
    span_ids = split_strings(span_id_out.data, span_id_out.string_lengths)
    parent_span_ids = split_strings(parent_span_id_out.data, parent_span_id_out.string_lengths)
    names = split_strings(name_out.data, name_out.string_lengths)
    status_messages = split_strings(status_message_out.data, status_message_out.string_lengths)

    # Detect batched vs legacy per-entry rest blob format
    rest_tuples = deserialize_rest_blobs(rest_blob_out.data, rest_blob_out.string_lengths, n)

    Enum.zip_with(
      [
        start_times,
        end_times,
        durations,
        kinds,
        statuses,
        trace_ids,
        span_ids,
        parent_span_ids,
        names,
        status_messages,
        rest_tuples
      ],
      fn [st, et, dur, ki, sta, tid, sid, psid, nm, sm, {attributes, events, resource, instrumentation_scope}] ->
        %{
          start_time: st,
          end_time: et,
          duration_ns: dur,
          kind: Map.get(@u8_to_kind, ki, :internal),
          status: Map.get(@u8_to_status, sta, :unset),
          trace_id: tid,
          span_id: sid,
          parent_span_id: if(psid == "", do: nil, else: psid),
          name: nm,
          status_message: if(sm == "", do: nil, else: sm),
          attributes: attributes,
          events: events,
          resource: resource,
          instrumentation_scope: instrumentation_scope
        }
      end
    )
  end

  # Batched format: single term_to_binary blob containing list of all rest tuples
  defp deserialize_rest_blobs(data, lengths_bin, n) when is_binary(lengths_bin) do
    lengths = for <<len::native-unsigned-32 <- lengths_bin>>, do: len

    cond do
      length(lengths) == 1 and n > 1 ->
        # Batched format
        :erlang.binary_to_term(data)

      length(lengths) == 1 ->
        # Single entry - could be either format
        result = :erlang.binary_to_term(data)
        if is_list(result), do: result, else: [result]

      true ->
        # Legacy per-entry format
        {blobs, _} =
          Enum.map_reduce(lengths, 0, fn len, offset ->
            {binary_part(data, offset, len), offset + len}
          end)

        Enum.map(blobs, &:erlang.binary_to_term/1)
    end
  end

  defp unpack_u64s(binary, n) do
    for <<val::little-unsigned-64 <- :binary.part(binary, 0, n * 8)>>, do: val
  end

  defp unpack_u8s(binary, n) do
    for <<val::unsigned-8 <- :binary.part(binary, 0, n)>>, do: val
  end

  defp split_strings(data, lengths_bin) when is_binary(lengths_bin) do
    lengths = for <<len::native-unsigned-32 <- lengths_bin>>, do: len

    {strings, _} =
      Enum.map_reduce(lengths, 0, fn len, offset ->
        {binary_part(data, offset, len), offset + len}
      end)

    strings
  end
end
