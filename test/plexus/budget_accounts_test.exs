defmodule Plexus.BudgetAccountsTest do
  use ExUnit.Case, async: true
  alias Plexus.{Budget, Run}
  alias Plexus.Budget.Accounts

  test "grants reserve parent credits, siblings transfer unused credit, close refunds only unused credit" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, budgets: [measure: 100])

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    assert :ok = Accounts.grant(id, :root, :a, measure: 20)
    assert :ok = Accounts.grant(id, :root, :b, measure: 10)
    assert :ok = Accounts.reserve(id, :a, :measure, 5)
    assert :ok = Accounts.transfer(id, :a, :b, :measure, 10)
    assert {:error, :budget_exhausted} = Accounts.transfer(id, :a, :b, :measure, 6)
    assert :ok = Accounts.grant(id, :b, :grandchild, measure: 8)
    assert {:error, :open_children} = Accounts.close(id, :b)
    assert :ok = Accounts.reserve(id, :grandchild, :measure, 3)
    assert :ok = Accounts.close(id, :grandchild)
    assert :ok = Accounts.close(id, :a)
    assert :ok = Accounts.close(id, :b)
    assert Budget.used(Run.config(run).budget, :measure) == 8
    assert {:error, :unknown_account} = Accounts.close(id, :b)
  end

  test "failed multi-meter grant rolls back all parent reservations" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, budgets: [measure: 3, expand: 0])

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    assert {:error, :budget_exhausted} = Accounts.grant(id, :root, :a, measure: 3, expand: 1)
    assert Budget.used(Run.config(run).budget, :measure) == 0
  end

  test "refunds cannot release credits still granted to descendants" do
    client = TypeSafeSDK.Test.client()
    {:ok, run} = Plexus.start_run(client: client, budgets: [measure: 100])

    on_exit(fn ->
      Plexus.stop_run(run)
      TypeSafeSDK.Test.close(client)
    end)

    id = Run.run_id(run)
    Accounts.grant(id, :root, :parent, measure: 20)
    Accounts.grant(id, :parent, :child, measure: 20)
    assert {:error, :credits_granted_to_children} = Accounts.refund(id, :parent, :measure, 1)
    assert {:error, :budget_exhausted} = Accounts.reserve(id, :parent, :measure, 1)
  end
end
