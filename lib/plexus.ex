defmodule Plexus do
  @moduledoc """
  Public API for Plexus.

  Plexus is a thin semantic actor substrate built directly on `TypeSafeSDK` 0.4.0.
  It starts bounded runs, supervises semantic actors, tracks subtree metadata, and
  provides contract helpers for shared TypeSafe evaluations.
  """

  alias Plexus.{Contract, Graph, Run}

  @type run_ref :: pid() | atom()
  @type actor_ref :: pid() | atom() | term()

  @spec start_run(keyword()) :: DynamicSupervisor.on_start_child()
  defdelegate start_run(opts), to: Run

  @spec stop_run(run_ref(), term()) :: :ok
  defdelegate stop_run(run, reason \\ :normal), to: Run

  @spec start_actor(run_ref(), keyword()) :: DynamicSupervisor.on_start_child()
  defdelegate start_actor(run, opts), to: Run

  @spec actor_pid(run_ref(), term()) :: {:ok, pid()} | {:error, :not_found}
  defdelegate actor_pid(run, actor_id), to: Run

  @spec cast(actor_ref(), term()) :: :ok
  def cast(pid, message) when is_pid(pid), do: GenServer.cast(pid, message)
  def cast({run, actor_id}, message), do: Run.cast(run, actor_id, message)

  @spec call(actor_ref(), term(), timeout()) :: term()
  def call(actor_ref, message, timeout \\ 5_000)
  def call(pid, message, timeout) when is_pid(pid), do: GenServer.call(pid, message, timeout)
  def call({run, actor_id}, message, timeout), do: Run.call(run, actor_id, message, timeout)

  @spec subtree(run_ref(), term()) :: [term()]
  def subtree(run, actor_id), do: Graph.subtree(Run.run_id(run), actor_id)

  @spec contract(keyword()) :: TypeSafeSDK.Prepared.t()
  defdelegate contract(questions), to: Contract, as: :new!
end
