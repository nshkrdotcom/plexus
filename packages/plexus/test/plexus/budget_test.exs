defmodule Plexus.BudgetTest do
  use ExUnit.Case, async: true

  alias Plexus.Budget

  test "reserve is atomic and bounded while observed usage can exceed an admission limit" do
    budget = Budget.new(measure: 2, tokens: 3)

    assert :ok = Budget.reserve(budget, :measure, 1)
    assert :ok = Budget.reserve(budget, :measure, 1)
    assert {:error, :budget_exhausted} = Budget.reserve(budget, :measure, 1)
    assert :ok = Budget.refund(budget, :measure, 1)
    assert 1 == Budget.used(budget, :measure)

    assert :ok = Budget.consume(budget, :tokens, 5)
    assert 5 == Budget.used(budget, :tokens)
    assert 0 == Budget.remaining(budget, :tokens)
  end
end
