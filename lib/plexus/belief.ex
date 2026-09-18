defmodule Plexus.Belief do
  @moduledoc """
  Typed local belief projection for TypeSafe noul, choice and score answers.

  Raw provider values are retained. Calibration, when supplied, is an explicit
  transform and never rewrites the raw value.
  """

  alias TypeSafeSDK.{ChoiceAnswer, NoulAnswer, Response, ScoreAnswer}

  @enforce_keys [:kind, :raw]
  defstruct [:kind, :raw, :value, :distribution, :calibrated, :upper, metadata: %{}]

  @type t :: %__MODULE__{
          kind: :bernoulli | :categorical | :ordinal,
          raw: term(),
          value: term(),
          distribution: map() | nil,
          calibrated: term() | nil,
          upper: number() | nil,
          metadata: map()
        }

  @spec from(term(), term(), keyword()) :: t()
  def from(response, answer_id, opts \\ []) do
    response
    |> Response.fetch!(answer_id)
    |> from_answer(opts)
  end

  @spec from_answer(term(), keyword()) :: t()
  def from_answer(answer, opts \\ [])

  def from_answer(%NoulAnswer{noul: p} = answer, opts) do
    calibrated = calibr(p, Keyword.get(opts, :calibration))
    %__MODULE__{kind: :bernoulli, raw: answer, value: p, calibrated: calibrated, upper: calibrated || p}
  end

  def from_answer(%ChoiceAnswer{choice: choice, probabilities: probabilities} = answer, _opts) do
    %__MODULE__{kind: :categorical, raw: answer, value: choice, distribution: probabilities}
  end

  def from_answer(%ScoreAnswer{score: score, probabilities: probabilities} = answer, _opts) do
    %__MODULE__{kind: :ordinal, raw: answer, value: score, distribution: probabilities}
  end

  @spec probability(t()) :: number() | nil
  def probability(%__MODULE__{kind: :bernoulli, calibrated: p}) when is_number(p), do: p
  def probability(%__MODULE__{kind: :bernoulli, value: p}), do: p
  def probability(_), do: nil

  @spec entropy(t()) :: float()
  def entropy(%__MODULE__{kind: :bernoulli} = belief) do
    p = probability(belief)
    shannon([p, 1.0 - p])
  end

  def entropy(%__MODULE__{distribution: distribution}) when is_map(distribution) do
    distribution |> Map.values() |> shannon()
  end

  def entropy(_), do: 0.0

  @spec combine(t(), t(), keyword()) :: t()
  def combine(left, right, opts \\ [])

  def combine(%__MODULE__{kind: :bernoulli} = left, %__MODULE__{kind: :bernoulli} = right, opts) do
    damping = Keyword.get(opts, :damping, 0.5)

    unless is_number(damping) and damping >= 0.0 and damping <= 1.0 do
      raise ArgumentError, "damping must be in [0, 1]"
    end

    p = probability(left) * (1.0 - damping) + probability(right) * damping
    %__MODULE__{kind: :bernoulli, raw: {left.raw, right.raw}, value: p, calibrated: p, upper: p}
  end

  def combine(%__MODULE__{kind: kind, distribution: a} = left, %__MODULE__{kind: kind, distribution: b}, opts)
      when kind in [:categorical, :ordinal] and is_map(a) and is_map(b) do
    damping = Keyword.get(opts, :damping, 0.5)
    keys = Map.keys(a) ++ Map.keys(b) |> Enum.uniq()

    distribution =
      Map.new(keys, fn key ->
        {key, Map.get(a, key, 0.0) * (1.0 - damping) + Map.get(b, key, 0.0) * damping}
      end)

    %{left | distribution: distribution}
  end

  defp calibr(_p, nil), do: nil
  defp calibr(p, fun) when is_function(fun, 1), do: fun.(p)
  defp calibr(p, model), do: Plexus.Belief.Calibration.apply(model, p)

  defp shannon(probabilities) do
    Enum.reduce(probabilities, 0.0, fn
      p, acc when is_number(p) and p > 0.0 -> acc - p * :math.log2(p)
      _, acc -> acc
    end)
  end
end
