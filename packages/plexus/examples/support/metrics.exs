defmodule Plexus.Examples.Support.Metrics do
  @moduledoc false

  def mean([]), do: 0.0
  def mean(values), do: Enum.sum(values) / length(values)

  def pct(_part, 0), do: 0.0
  def pct(part, whole), do: 100.0 * part / whole

  def print_table(rows) do
    rows
    |> Enum.each(fn {label, value} ->
      IO.puts(String.pad_trailing(to_string(label), 28) <> to_string(value))
    end)
  end
end
