defmodule Plexus.Examples.IncidentCommander.Chronology do
  @moduledoc false

  alias Plexus.Examples.Support.Data

  @timestamp_fields ["timestamp", "datetime", "start_time", "startTime", "time"]

  def open!(trace_files, business_files, opts)
      when is_list(trace_files) and is_list(business_files) do
    day = Keyword.fetch!(opts, :day)

    sources =
      (Enum.map(trace_files, &{:trace, &1}) ++ Enum.map(business_files, &{:business, &1}))
      |> Enum.sort_by(fn {kind, path} -> {kind_rank(kind), path} end)
      |> Enum.with_index()
      |> Enum.map(fn {{kind, path}, index} -> open_source!(kind, path, index, day) end)
      |> Enum.reject(&is_nil/1)

    %{sources: sources}
  end

  def next(%{sources: []}), do: :eof

  def next(%{sources: sources} = cursor) do
    {source, index} =
      sources
      |> Enum.with_index()
      |> Enum.min_by(fn {source, _index} -> source.head.order end)

    event = Map.delete(source.head, :order)

    sources =
      case advance(source) do
        nil -> List.delete_at(sources, index)
        advanced -> List.replace_at(sources, index, advanced)
      end

    {:ok, event, %{cursor | sources: sources}}
  end

  defp open_source!(kind, path, index, day) do
    stream =
      path
      |> Data.csv_maps!()
      |> Stream.filter(&selected_day?(&1, day))

    case Enumerable.reduce(stream, {:cont, nil}, &suspend_row/2) do
      {:suspended, row, continuation} ->
        %{
          kind: kind,
          path: path,
          index: index,
          ordinal: 1,
          continuation: continuation,
          head: normalize!(kind, path, index, 1, row)
        }

      {:done, _acc} ->
        nil

      {:halted, _acc} ->
        nil
    end
  end

  defp advance(source) do
    case source.continuation.({:cont, nil}) do
      {:suspended, row, continuation} ->
        ordinal = source.ordinal + 1

        %{
          source
          | ordinal: ordinal,
            continuation: continuation,
            head: normalize!(source.kind, source.path, source.index, ordinal, row)
        }

      {:done, _acc} ->
        nil

      {:halted, _acc} ->
        nil
    end
  end

  defp suspend_row(row, _acc), do: {:suspend, row}

  defp selected_day?(row, day) do
    case timestamp(row) do
      value when is_binary(value) -> String.starts_with?(value, day)
      _ -> false
    end
  end

  defp normalize!(kind, path, source_index, ordinal, row) do
    timestamp = event_timestamp!(kind, row, path)
    event_time_us = parse_time_us!(timestamp, path)
    service = service(row)

    if service in [nil, ""] do
      raise "GAIA row is missing a service name in #{path}: #{inspect(Map.take(row, @timestamp_fields))}"
    end

    event_id =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary(
          {kind, path, ordinal, timestamp, service, row["trace_id"], row["span_id"]},
          [:deterministic]
        )
      )
      |> Base.encode16(case: :lower)

    %{
      order: {event_time_us, source_index, ordinal},
      event_id: event_id,
      source: kind,
      event_time_us: event_time_us,
      timestamp: timestamp,
      service: service,
      raw: row
    }
  end

  defp event_timestamp!(:business, row, path) do
    raw_timestamp = timestamp(row)

    cond do
      date_only?(raw_timestamp) ->
        business_message_timestamp(row["message"]) ||
          raise(
            "GAIA business row has date-only datetime #{inspect(raw_timestamp)} " <>
              "but its message contains no event timestamp in #{path}"
          )

      is_binary(raw_timestamp) and raw_timestamp != "" ->
        raw_timestamp

      true ->
        business_message_timestamp(row["message"]) ||
          raise("GAIA business row is missing an event timestamp in #{path}")
    end
  end

  defp event_timestamp!(_kind, row, path) do
    timestamp(row) || raise("GAIA row is missing a timestamp in #{path}")
  end

  defp date_only?(value) when is_binary(value) do
    Regex.match?(~r/^\d{4}-\d{2}-\d{2}$/, value)
  end

  defp date_only?(_value), do: false

  defp business_message_timestamp(message) when is_binary(message) do
    case Regex.run(
           ~r/(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[,.]\d{1,6})?)/,
           message,
           capture: :all_but_first
         ) do
      [value] -> String.replace(value, ",", ".")
      _ -> nil
    end
  end

  defp business_message_timestamp(_message), do: nil

  defp timestamp(row) do
    Enum.find_value(@timestamp_fields, fn field ->
      case row[field] do
        value when is_binary(value) and value != "" -> value
        _ -> nil
      end
    end)
  end

  defp service(row) do
    Enum.find_value(["service_name", "service", "serviceName"], fn field ->
      case row[field] do
        value when is_binary(value) and value != "" -> value
        _ -> nil
      end
    end)
  end

  defp parse_time_us!(value, path) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        DateTime.to_unix(datetime, :microsecond)

      _ ->
        parse_naive_or_numeric!(value, path)
    end
  end

  defp parse_naive_or_numeric!(value, path) do
    normalized = String.replace(value, " ", "T")

    case NaiveDateTime.from_iso8601(normalized) do
      {:ok, naive} ->
        naive
        |> DateTime.from_naive!("Etc/UTC")
        |> DateTime.to_unix(:microsecond)

      _ ->
        parse_numeric_time!(value, path)
    end
  end

  defp parse_numeric_time!(value, path) do
    case Float.parse(value) do
      {number, ""} when number >= 100_000_000_000_000 -> trunc(number)
      {number, ""} when number >= 100_000_000_000 -> trunc(number * 1_000)
      {number, ""} when number >= 1_000_000_000 -> trunc(number * 1_000_000)
      _ -> raise "unsupported GAIA timestamp #{inspect(value)} in #{path}"
    end
  end

  defp kind_rank(:trace), do: 0
  defp kind_rank(:business), do: 1
end
