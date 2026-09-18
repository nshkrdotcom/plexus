defmodule Plexus.Registry do
  @moduledoc """
  Small wrapper around the process registry used for run-scoped actor lookup.
  """

  @spec via(term(), term()) :: {:via, Registry, {module(), term()}}
  def via(run_id, actor_id), do: {:via, Registry, {__MODULE__, {run_id, actor_id}}}

  @spec register(term(), term(), pid()) :: {:ok, pid()} | {:error, {:already_registered, pid()}}
  def register(run_id, actor_id, pid) do
    case Registry.register(__MODULE__, {run_id, actor_id}, pid) do
      {:ok, _} -> {:ok, pid}
      {:error, {:already_registered, pid}} = error -> error
    end
  end

  @spec lookup(term(), term()) :: {:ok, pid()} | {:error, :not_found}
  def lookup(run_id, actor_id) do
    case Registry.lookup(__MODULE__, {run_id, actor_id}) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end
end
