defmodule Plexus.Belief.Calibration do
  @moduledoc """
  Calibration diagnostics and a dependency-free isotonic calibrator.

  Samples are `{predicted_probability, observed_boolean}` pairs. The reliability
  output is intended for experiment artifacts; the isotonic model is monotone
  and can be applied at runtime without treating raw noul values as calibrated
  likelihoods.
  """

  @type sample :: {number(), boolean() | 0 | 1}
  @type model :: %{method: :isotonic, blocks: [map()]}

  @spec reliability([sample()], pos_integer()) :: [map()]
  def reliability(samples, bins \\ 10) when is_integer(bins) and bins > 0 do
    samples
    |> validate_samples!()
    |> Enum.group_by(fn {p, _} -> min(trunc(p * bins), bins - 1) end)
    |> Enum.map(fn {index, rows} ->
      predicted = average(Enum.map(rows, &elem(&1, 0)))
      observed = average(Enum.map(rows, &(if truth?(elem(&1, 1)), do: 1.0, else: 0.0)))

      %{
        bin: index,
        lower: index / bins,
        upper: (index + 1) / bins,
        count: length(rows),
        mean_predicted: predicted,
        observed_rate: observed,
        absolute_gap: abs(predicted - observed)
      }
    end)
    |> Enum.sort_by(& &1.bin)
  end

  @spec expected_calibration_error([sample()], pos_integer()) :: float()
  def expected_calibration_error(samples, bins \\ 10) do
    total = max(length(samples), 1)

    samples
    |> reliability(bins)
    |> Enum.reduce(0.0, fn row, acc -> acc + row.count / total * row.absolute_gap end)
  end

  @spec fit_isotonic([sample()]) :: model()
  def fit_isotonic(samples) do
    blocks =
      samples
      |> validate_samples!()
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {p, observed} ->
        y = if truth?(observed), do: 1.0, else: 0.0
        %{min: p, max: p, weight: 1, sum: y, value: y}
      end)
      |> pava([])
      |> Enum.reverse()

    %{method: :isotonic, blocks: blocks}
  end

  @spec apply(model(), number()) :: float()
  def apply(%{method: :isotonic, blocks: []}, p), do: clamp(p)

  def apply(%{method: :isotonic, blocks: blocks}, p) when is_number(p) do
    p = clamp(p)

    case Enum.find(blocks, &(p <= &1.max)) do
      nil -> List.last(blocks).value
      block -> block.value
    end
  end

  defp pava([], stack), do: stack

  defp pava([block | rest], stack) do
    stack = merge_violations([block | stack])
    pava(rest, stack)
  end

  # Stack is newest-first; if the older block has a larger fitted value than the
  # newer one, merge them until monotonicity is restored.
  defp merge_violations([newer, older | rest]) when older.value > newer.value do
    weight = older.weight + newer.weight
    sum = older.sum + newer.sum

    merged = %{
      min: older.min,
      max: newer.max,
      weight: weight,
      sum: sum,
      value: sum / weight
    }

    merge_violations([merged | rest])
  end

  defp merge_violations(stack), do: stack

  defp validate_samples!(samples) do
    Enum.map(samples, fn
      {p, observed} when is_number(p) and p >= 0.0 and p <= 1.0 and observed in [true, false, 0, 1] -> {p * 1.0, observed}
      sample -> raise ArgumentError, "invalid calibration sample: #{inspect(sample)}"
    end)
  end

  defp truth?(value), do: value in [true, 1]
  defp average([]), do: 0.0
  defp average(values), do: Enum.sum(values) / length(values)
  defp clamp(p), do: min(max(p * 1.0, 0.0), 1.0)
end
