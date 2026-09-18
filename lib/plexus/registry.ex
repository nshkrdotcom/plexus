defmodule Plexus.Registry do
  @moduledoc """
  Run-scoped process addressing on a partitioned `Registry`.
  """

  alias Plexus.Run.Names

  @spec via(term(), term()) :: {:via, Registry, {module(), term()}}
  def via(run_id, actor_id), do: Names.actor(run_id, actor_id)

  @spec lookup(term(), term()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(run_id, actor_id), do: lookup_key({:actor, run_id, actor_id})

  @spec lookup_run(term()) :: {:ok, pid()} | {:error, :not_found}
  def lookup_run(run_id), do: lookup_key({:run_owner, run_id})

  @spec run_id(pid()) :: {:ok, term()} | {:error, :not_found}
  def run_id(pid) when is_pid(pid) do
    Plexus.Registry
    |> Registry.keys(pid)
    |> Enum.find_value({:error, :not_found}, fn
      {:run_owner, run_id} -> {:ok, run_id}
      _ -> false
    end)
  end

  defp lookup_key(key) do
    case Registry.lookup(__MODULE__, key) do
      [{pid, _value}] -> if Process.alive?(pid), do: {:ok, pid}, else: {:error, :not_found}
      [] -> {:error, :not_found}
    end
  end
end
