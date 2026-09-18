defmodule Plexus do
  @moduledoc """
  Single-node kernel for massively asynchronous semantic computation.

  Plexus keeps TypeSafeSDK as the semantic measurement engine and adds run-scoped
  population/topology, declarative actor effects, batching/dedupe, budgets,
  scheduling regimes, replay and an explicit expansion seam.
  """

  alias Plexus.{Actor, Budget, Contract, Graph, Record, Run, Schedule}
  alias Plexus.Contract.Registry, as: ContractRegistry

  @type run_ref :: pid() | term()
  @type actor_ref :: pid() | {run_ref(), term()}

  @spec start_run(keyword()) :: {:ok, pid()} | {:error, term()}
  defdelegate start_run(opts), to: Run

  @spec stop_run(run_ref(), term()) :: :ok | {:error, :not_found}
  defdelegate stop_run(run, reason \\ :normal), to: Run

  @spec start_actor(run_ref(), keyword()) :: DynamicSupervisor.on_start_child()
  defdelegate start_actor(run, opts), to: Run

  @spec actor_pid(run_ref(), term()) :: {:ok, pid()} | {:error, :not_found}
  defdelegate actor_pid(run, actor_id), to: Run

  @spec cast(actor_ref(), term()) :: :ok | {:error, :not_found}
  def cast(pid, message) when is_pid(pid), do: GenServer.cast(pid, message)
  def cast({run, actor_id}, message), do: Run.cast(run, actor_id, message)

  @spec call(actor_ref(), term(), timeout()) :: term()
  def call(actor_ref, message, timeout \\ 5_000)
  def call(pid, message, timeout) when is_pid(pid), do: GenServer.call(pid, message, timeout)
  def call({run, actor_id}, message, timeout), do: Run.call(run, actor_id, message, timeout)

  @spec dispatch(map(), Plexus.Actor.Command.t() | [Plexus.Actor.Command.t()]) :: :ok
  defdelegate dispatch(context, commands), to: Actor

  @spec subtree(run_ref(), term()) :: [term()]
  def subtree(run, actor_id), do: Graph.subtree(Run.run_id(run), actor_id)

  @spec prune(run_ref(), term()) :: :ok
  defdelegate prune(run, actor_id), to: Run

  @spec contract(keyword()) :: TypeSafeSDK.Prepared.t()
  defdelegate contract(questions), to: Contract, as: :new!

  @spec register_contract(run_ref(), term(), TypeSafeSDK.Prepared.t() | keyword(), keyword()) ::
          :ok
  def register_contract(run, name, prepared_or_questions, opts \\ []) do
    ContractRegistry.put(Run.run_id(run), name, prepared_or_questions, opts)
  end

  @spec barrier(run_ref()) :: {:ok, non_neg_integer(), non_neg_integer()}
  def barrier(run), do: Schedule.barrier(Run.run_id(run))

  @spec schedule(run_ref(), Plexus.Schedule.regime()) :: :ok
  def schedule(run, regime), do: Schedule.set_regime(Run.run_id(run), regime)

  @spec budget(run_ref()) :: map()
  def budget(run), do: Budget.snapshot(Run.config(run).budget)

  @spec events(run_ref()) :: [map()]
  def events(run), do: Record.events(Run.run_id(run))
  @spec replay_entries(run_ref()) :: [{String.t(), term()}]
  def replay_entries(run), do: Record.replay_entries(Run.run_id(run))

  @spec load_replay(run_ref(), Enumerable.t()) :: :ok
  def load_replay(run, entries), do: Record.load_replay(Run.run_id(run), entries)

  @spec publish(run_ref(), term(), term()) :: non_neg_integer()
  def publish(run, event, payload \\ nil),
    do: Plexus.Event.publish(Run.run_id(run), event, payload)
end
