defmodule Plexus.Examples.Support.Data do
  @moduledoc false

  def jsonl!(path) do
    path
    |> File.stream!([], :line)
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.map(&Jason.decode!/1)
  end

  def write_jsonl!(path, rows) do
    File.mkdir_p!(Path.dirname(path))

    File.open!(path, [:write], fn io ->
      Enum.each(rows, &IO.write(io, Jason.encode!(&1) <> "\n"))
    end)

    path
  end

  def csv_maps!(path) do
    stream = File.stream!(path, [], :line)

    Stream.transform(stream, %{buffer: "", header: nil}, fn line, state ->
      buffer = state.buffer <> line

      if complete_record?(buffer) do
        fields = parse_record(buffer)

        case state.header do
          nil -> {[], %{state | buffer: "", header: fields}}
          header -> {[Map.new(Enum.zip(header, fields))], %{state | buffer: ""}}
        end
      else
        {[], %{state | buffer: buffer}}
      end
    end)
  end

  def gunzip!(source, target) do
    source |> File.read!() |> :zlib.gunzip() |> then(&File.write!(target, &1))
    target
  end

  def extract_tar_gz!(archive, destination) do
    File.mkdir_p!(destination)

    case :erl_tar.extract(String.to_charlist(archive), [
           :compressed,
           {:cwd, String.to_charlist(destination)}
         ]) do
      :ok -> destination
      {:error, reason} -> raise "could not extract #{archive}: #{inspect(reason)}"
    end
  end

  def parse_number(nil), do: nil
  def parse_number(""), do: nil
  def parse_number(value) when is_number(value), do: value

  def parse_number(value) do
    case Float.parse(to_string(value)) do
      {number, _} -> number
      :error -> nil
    end
  end

  defp complete_record?(binary), do: not quoted?(binary)

  defp quoted?(binary) do
    binary
    |> :binary.bin_to_list()
    |> Enum.reduce({false, false}, fn char, {quoted, pending_quote} ->
      cond do
        char == ?" and quoted and pending_quote -> {quoted, false}
        char == ?" and quoted -> {quoted, true}
        char == ?" and not quoted -> {true, false}
        pending_quote -> {false, false}
        true -> {quoted, false}
      end
    end)
    |> then(fn {quoted, pending_quote} -> quoted and not pending_quote end)
  end

  defp parse_record(binary) do
    chars = binary |> String.trim_trailing() |> String.to_charlist()
    {fields, field, _quoted} = parse_chars(chars, [], [], false)
    Enum.reverse([field |> Enum.reverse() |> to_string() | fields])
  end

  defp parse_chars([], fields, field, quoted), do: {fields, field, quoted}

  defp parse_chars([?", ?" | rest], fields, field, true),
    do: parse_chars(rest, fields, [?" | field], true)

  defp parse_chars([?" | rest], fields, field, quoted),
    do: parse_chars(rest, fields, field, not quoted)

  defp parse_chars([?, | rest], fields, field, false) do
    value = field |> Enum.reverse() |> to_string()
    parse_chars(rest, [value | fields], [], false)
  end

  defp parse_chars([char | rest], fields, field, quoted),
    do: parse_chars(rest, fields, [char | field], quoted)
end
