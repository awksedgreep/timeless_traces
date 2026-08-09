defmodule Mix.Tasks.LegacyOzl.Transcode do
  @shortdoc "Rewrite pre-0.4.7 OpenZL trace blocks as version-independent :raw blocks"

  @moduledoc """
      mix legacy_ozl.transcode SRC DEST

  Decodes every `*.ozl` block in SRC with OpenZL 0.1.x and writes a `*.raw`
  block into DEST. Originals are not modified.

  Prints the `UPDATE` statements needed to point `traces_index.db` at the new
  blocks. It deliberately does not run them: the index is live data, and the
  operator should apply the change with the store stopped and a backup taken.
  """

  use Mix.Task

  @impl Mix.Task
  def run(argv) do
    {src, dest} =
      case argv do
        [src, dest] -> {src, dest}
        _ -> Mix.raise("usage: mix legacy_ozl.transcode SRC DEST")
      end

    unless File.dir?(src), do: Mix.raise("no such source directory: #{src}")

    Mix.shell().info("transcoding blocks from #{src}\n")

    results = LegacyOzlTranscode.transcode_dir(src, dest)

    Enum.each(results, fn
      {:ok, name, spans, bytes} ->
        Mix.shell().info("  OK   #{name}: #{spans} spans -> #{bytes} bytes")

      {:error, name, reason} ->
        Mix.shell().error("  FAIL #{name}: #{String.slice(to_string(reason), 0, 160)}")
    end)

    ok = Enum.filter(results, &match?({:ok, _, _, _}, &1))
    failed = length(results) - length(ok)
    spans = ok |> Enum.map(&elem(&1, 2)) |> Enum.sum()

    Mix.shell().info(
      "\n#{length(ok)}/#{length(results)} blocks transcoded, #{spans} spans recovered"
    )

    {numbered, unnumbered} = Enum.split_with(ok, fn {:ok, name, _, _} -> block_id(name) end)

    if numbered != [] do
      Mix.shell().info("\n-- apply with the store stopped, after backing up traces_index.db:")

      Enum.each(numbered, fn {:ok, name, _spans, bytes} ->
        Mix.shell().info(
          "UPDATE blocks SET format = 'raw', " <>
            "file_path = replace(file_path, '#{name}.ozl', '#{name}.raw'), " <>
            "byte_size = #{bytes} WHERE block_id = #{block_id(name)};"
        )
      end)
    end

    # A block file is named for its block_id. Anything else did not come from
    # the writer, so we cannot say which index row it belongs to and must not
    # guess one into an UPDATE.
    if unnumbered != [] do
      Mix.shell().info("")

      Enum.each(unnumbered, fn {:ok, name, _, _} ->
        Mix.shell().error(
          "-- no statement for #{name}.raw: filename is not a block id, " <>
            "so its index row cannot be identified"
        )
      end)
    end

    if failed > 0 do
      Mix.raise("#{failed} block(s) could not be decoded; nothing should be applied")
    end
  end

  defp block_id(name) do
    case Integer.parse(name) do
      {id, ""} -> id
      _ -> nil
    end
  end
end
