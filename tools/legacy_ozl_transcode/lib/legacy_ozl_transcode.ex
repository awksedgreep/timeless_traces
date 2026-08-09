defmodule LegacyOzlTranscode do
  @moduledoc """
  Recovers trace blocks written by `ex_openzl <= 0.4.6` and rewrites them in a
  format that carries no OpenZL dependency.

  `ex_openzl 0.4.7` moved its vendored OpenZL from 0.1.x to v0.2.0, and the
  0.2.0 decoder rejects 0.1.x frames. The stored bytes are intact — they
  still introspect cleanly — but the current build cannot read them, which
  blocks the legacy-to-libSQL migration.

  Rather than teach the running application to speak two OpenZL versions, this
  decodes each block with the old decoder and rewrites it as a `:raw` block:
  `:erlang.term_to_binary/1` over the same span maps. `:raw` is one of the
  formats `TimelessTraces.Writer.decompress_block/2` already accepts and
  involves no compression library at all, so it cannot go stale the same way.

  Blocks are never modified in place. Output goes to a separate directory and
  the originals are left untouched.

  ## Keeping in step with the writer

  The columnar layout below is vendored from `TimelessTraces.Writer`, because
  this tool cannot depend on `timeless_traces` (that would pull in the newer
  ex_openzl). If the writer's column order or rest-blob encoding changes, this
  must change with it.

  The check that matters is end to end: after transcoding, `timeless_traces`
  itself must read the output and produce well-formed spans. See the README.
  """

  @u8_to_kind %{0 => :internal, 1 => :server, 2 => :client, 3 => :producer, 4 => :consumer}
  @u8_to_status %{0 => :ok, 1 => :error, 2 => :unset}

  @doc """
  Decode one legacy OpenZL block into span maps.
  """
  @spec decode_block(binary()) :: {:ok, [map()]} | {:error, term()}
  def decode_block(binary) when is_binary(binary) do
    {:ok, ctx} = ExOpenzl.create_decompression_context()

    case ExOpenzl.decompress_multi_typed(ctx, binary) do
      {:ok, outputs} -> {:ok, columnar_deserialize(outputs)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  @doc """
  Transcode every `*.ozl` file in `src` into a `*.raw` file in `dest`.

  Returns one result per block, in filename order.
  """
  @spec transcode_dir(Path.t(), Path.t()) :: [
          {:ok, String.t(), non_neg_integer(), non_neg_integer()}
          | {:error, String.t(), term()}
        ]
  def transcode_dir(src, dest) do
    File.mkdir_p!(dest)

    src
    |> Path.join("*.ozl")
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(&transcode_file(&1, dest))
  end

  defp transcode_file(path, dest) do
    name = Path.basename(path, ".ozl")

    case decode_block(File.read!(path)) do
      {:ok, spans} ->
        encoded = :erlang.term_to_binary(spans)
        File.write!(Path.join(dest, "#{name}.raw"), encoded)
        {:ok, name, length(spans), byte_size(encoded)}

      {:error, reason} ->
        {:error, name, reason}
    end
  end

  # --- vendored from TimelessTraces.Writer ---------------------------------

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
      fn [
           st,
           et,
           dur,
           ki,
           sta,
           tid,
           sid,
           psid,
           nm,
           sm,
           {attributes, events, resource, instrumentation_scope}
         ] ->
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

  defp deserialize_rest_blobs(data, lengths_bin, n) when is_binary(lengths_bin) do
    lengths = for <<len::native-unsigned-32 <- lengths_bin>>, do: len

    cond do
      length(lengths) == 1 and n > 1 ->
        :erlang.binary_to_term(data)

      length(lengths) == 1 ->
        result = :erlang.binary_to_term(data)
        if is_list(result), do: result, else: [result]

      true ->
        {blobs, _} =
          Enum.map_reduce(lengths, 0, fn len, offset ->
            {binary_part(data, offset, len), offset + len}
          end)

        Enum.map(blobs, &:erlang.binary_to_term/1)
    end
  end

  defp unpack_u64s(binary, n),
    do: for(<<val::little-unsigned-64 <- :binary.part(binary, 0, n * 8)>>, do: val)

  defp unpack_u8s(binary, n),
    do: for(<<val::unsigned-8 <- :binary.part(binary, 0, n)>>, do: val)

  defp split_strings(data, lengths_bin) when is_binary(lengths_bin) do
    lengths = for <<len::native-unsigned-32 <- lengths_bin>>, do: len

    {strings, _} =
      Enum.map_reduce(lengths, 0, fn len, offset ->
        {binary_part(data, offset, len), offset + len}
      end)

    strings
  end
end
