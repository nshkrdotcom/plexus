defmodule Plexus.Budget.Accounts do
  @moduledoc """
  Run-local hierarchical credit grants.

  Grants reserve finite credits in the parent up front. Reservations in a child
  spend its grant, sibling transfers move only unused credit, and closing a leaf
  returns unused credit while retaining consumed work in its parent. The root is
  the run's atomic budget; ordinary measurements keep that fast path. Scoped
  credit management is serialized within this run, never globally.
  """
  use GenServer
  alias Plexus.Budget
  alias Plexus.Run.{Config, Names}
  @meters [:measure, :expand, :tokens, :population]

  def start_link(opts) do
    id = Keyword.fetch!(opts, :run_id)
    GenServer.start_link(__MODULE__, id, name: name(id))
  end

  @spec grant(term(), term(), term(), keyword() | map()) :: :ok | {:error, term()}
  def grant(run_id, parent, child, limits),
    do: GenServer.call(name(run_id), {:grant, parent, child, Map.new(limits)})

  @spec reserve(term(), term(), Budget.meter(), non_neg_integer()) :: :ok | {:error, term()}
  def reserve(run_id, account, meter, amount),
    do: GenServer.call(name(run_id), {:reserve, account, meter, amount})

  @spec refund(term(), term(), Budget.meter(), non_neg_integer()) :: :ok | {:error, term()}
  def refund(run_id, account, meter, amount),
    do: GenServer.call(name(run_id), {:refund, account, meter, amount})

  @spec transfer(term(), term(), term(), Budget.meter(), non_neg_integer()) ::
          :ok | {:error, term()}
  def transfer(run_id, from, to, meter, amount),
    do: GenServer.call(name(run_id), {:transfer, from, to, meter, amount})

  @spec close(term(), term()) :: :ok | {:error, term()}
  def close(run_id, account), do: GenServer.call(name(run_id), {:close, account})
  @spec snapshot(term()) :: map()
  def snapshot(run_id), do: GenServer.call(name(run_id), :snapshot)

  @impl true
  def init(run_id) do
    config = Config.fetch!(run_id)
    table = config.tables.budget_accounts

    accounts =
      case :ets.lookup(table, :accounts) do
        [{:accounts, accounts}] -> accounts
        [] -> %{root: %{parent: nil, budget: config.budget}}
      end

    {:ok, %{accounts: accounts, table: table}}
  end

  @impl true
  def handle_call(message, from, state) do
    case request(message, from, state.accounts) do
      {:reply, reply, accounts} ->
        :ets.insert(state.table, {:accounts, accounts})
        {:reply, reply, %{state | accounts: accounts}}

      {:close, entry, accounts} ->
        # Commit removal before releasing credits: interruption can retain credit,
        # but must never allow a restarted ledger to refund the same grant twice.
        :ets.insert(state.table, {:accounts, accounts})
        parent = Map.fetch!(accounts, entry.parent)
        Enum.each(@meters, &Budget.refund(parent.budget, &1, Budget.remaining(entry.budget, &1)))
        {:reply, :ok, %{state | accounts: accounts}}
    end
  end

  defp request({:grant, parent, child, limits}, _, state) do
    limits = Map.merge(Map.new(@meters, &{&1, 0}), limits)

    cond do
      Map.has_key?(state, child) -> {:reply, {:error, :account_exists}, state}
      not Map.has_key?(state, parent) -> {:reply, {:error, :unknown_account}, state}
      not valid_limits?(limits) -> {:reply, {:error, :invalid_limits}, state}
      true -> grant_child(state, parent, child, limits)
    end
  end

  defp request({action, account, meter, amount}, _, state)
       when action in [:reserve, :refund] do
    result =
      with {:ok, entry} <- fetch(state, account),
           :ok <- valid_amount(meter, amount),
           :ok <- refundable(state, account, meter, amount, action),
           do: apply(Budget, action, [entry.budget, meter, amount])

    {:reply, result, state}
  end

  defp request({:transfer, from, to, meter, amount}, _, state) do
    with {:ok, source} <- fetch(state, from),
         {:ok, target} <- fetch(state, to),
         :ok <- valid_amount(meter, amount),
         :ok <-
           check(from != to and from != :root and source.parent == target.parent, :not_siblings),
         :ok <- check(Budget.remaining(source.budget, meter) >= amount, :budget_exhausted) do
      source = update_in(source.budget.limits[meter], &(&1 - amount))
      target = update_in(target.budget.limits[meter], &(&1 + amount))
      {:reply, :ok, state |> Map.put(from, source) |> Map.put(to, target)}
    else
      error -> {:reply, error, state}
    end
  end

  defp request({:close, account}, _, state) do
    with {:ok, entry} <- fetch(state, account),
         :ok <- check(account != :root, :cannot_close_root),
         :ok <-
           check(
             not Enum.any?(state, fn {_, child} -> child.parent == account end),
             :open_children
           ) do
      {:close, entry, Map.delete(state, account)}
    else
      error -> {:reply, error, state}
    end
  end

  defp request(:snapshot, _, state) do
    {:reply,
     Map.new(state, fn {id, entry} ->
       {id, %{parent: entry.parent, meters: Budget.snapshot(entry.budget)}}
     end), state}
  end

  defp refundable(_state, _account, _meter, _amount, :reserve), do: :ok

  defp refundable(state, account, meter, amount, :refund) do
    granted =
      state
      |> Enum.filter(fn {_, entry} -> entry.parent == account end)
      |> Enum.map(fn {_, entry} -> entry.budget.limits[meter] end)
      |> Enum.sum()

    check(
      Budget.used(state[account].budget, meter) - amount >= granted,
      :credits_granted_to_children
    )
  end

  defp grant_child(state, parent, child, limits) do
    budget = state[parent].budget

    case reserve_grant(budget, Map.to_list(limits), []) do
      :ok -> {:reply, :ok, Map.put(state, child, %{parent: parent, budget: Budget.new(limits)})}
      error -> {:reply, error, state}
    end
  end

  defp reserve_grant(_budget, [], _reserved), do: :ok

  defp reserve_grant(budget, [{meter, amount} | rest], reserved) do
    case Budget.reserve(budget, meter, amount) do
      :ok ->
        reserve_grant(budget, rest, [{meter, amount} | reserved])

      error ->
        Enum.each(reserved, fn {m, n} -> Budget.refund(budget, m, n) end)
        error
    end
  end

  defp valid_limits?(limits),
    do: Enum.all?(limits, fn {m, n} -> m in @meters and is_integer(n) and n >= 0 end)

  defp valid_amount(m, n), do: check(m in @meters and is_integer(n) and n >= 0, :invalid_amount)
  defp check(true, _), do: :ok
  defp check(false, reason), do: {:error, reason}

  defp fetch(state, id) do
    case Map.fetch(state, id) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, :unknown_account}
    end
  end

  defp name(id), do: Names.via({:budget_accounts, id})
end
