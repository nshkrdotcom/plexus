defmodule Plexus.Budget do
  @moduledoc """
  Atomic per-run admission ledger.

  The ledger tracks logical measure work, expansion work, token usage and live
  population. Limits may be integers or `:infinity`.
  """

  @meters [:measure, :expand, :tokens, :population]
  @index @meters |> Enum.with_index(1) |> Map.new()

  @type meter :: :measure | :expand | :tokens | :population
  @type limits :: %{optional(meter()) => non_neg_integer() | :infinity}
  @type t :: %{atomics: reference(), limits: limits()}

  @spec new(keyword() | map()) :: t()
  def new(opts \\ []) do
    limits =
      Map.new(@meters, fn meter ->
        value = get_limit(opts, meter)

        unless value == :infinity or (is_integer(value) and value >= 0) do
          raise ArgumentError, "budget #{meter} must be a non-negative integer or :infinity"
        end

        {meter, value}
      end)

    %{atomics: :atomics.new(length(@meters), signed: false), limits: limits}
  end

  @spec reserve(t(), meter(), non_neg_integer()) :: :ok | {:error, :budget_exhausted}
  def reserve(_budget, _meter, 0), do: :ok

  def reserve(%{atomics: atomics, limits: limits}, meter, amount)
      when is_integer(amount) and amount > 0 do
    index = index!(meter)

    case Map.fetch!(limits, meter) do
      :infinity ->
        :atomics.add(atomics, index, amount)
        :ok

      limit ->
        reserve_bounded(atomics, index, limit, amount)
    end
  end

  @spec refund(t(), meter(), non_neg_integer()) :: :ok
  def refund(_budget, _meter, 0), do: :ok

  def refund(%{atomics: atomics}, meter, amount) when is_integer(amount) and amount > 0 do
    index = index!(meter)
    refund_loop(atomics, index, amount)
    :ok
  end

  @spec used(t(), meter()) :: non_neg_integer()
  def used(%{atomics: atomics}, meter), do: :atomics.get(atomics, index!(meter))

  @doc "Record already-consumed work without admission checking (for observed token usage)."
  @spec consume(t(), meter(), non_neg_integer()) :: :ok
  def consume(_budget, _meter, 0), do: :ok

  def consume(%{atomics: atomics}, meter, amount) when is_integer(amount) and amount > 0 do
    :atomics.add(atomics, index!(meter), amount)
    :ok
  end

  @spec remaining(t(), meter()) :: non_neg_integer() | :infinity
  def remaining(%{limits: limits} = budget, meter) do
    case Map.fetch!(limits, meter) do
      :infinity -> :infinity
      limit -> max(limit - used(budget, meter), 0)
    end
  end

  @spec snapshot(t()) :: map()
  def snapshot(budget) do
    Map.new(@meters, fn meter ->
      {meter, %{used: used(budget, meter), remaining: remaining(budget, meter)}}
    end)
  end

  @spec transfer(t(), meter(), non_neg_integer(), (-> term())) ::
          {:ok, term()} | {:error, :budget_exhausted}
  def transfer(budget, meter, amount, fun) when is_function(fun, 0) do
    case reserve(budget, meter, amount) do
      :ok -> {:ok, fun.()}
      {:error, :budget_exhausted} = error -> error
    end
  end

  defp reserve_bounded(atomics, index, limit, amount) do
    current = :atomics.get(atomics, index)

    if current + amount > limit do
      {:error, :budget_exhausted}
    else
      case :atomics.compare_exchange(atomics, index, current, current + amount) do
        :ok -> :ok
        _actual -> reserve_bounded(atomics, index, limit, amount)
      end
    end
  end

  defp refund_loop(atomics, index, amount) do
    current = :atomics.get(atomics, index)
    target = max(current - amount, 0)

    case :atomics.compare_exchange(atomics, index, current, target) do
      :ok -> :ok
      _actual -> refund_loop(atomics, index, amount)
    end
  end

  defp index!(meter), do: Map.fetch!(@index, meter)

  defp get_limit(opts, meter) when is_list(opts), do: Keyword.get(opts, meter, :infinity)
  defp get_limit(opts, meter) when is_map(opts), do: Map.get(opts, meter, :infinity)
end
