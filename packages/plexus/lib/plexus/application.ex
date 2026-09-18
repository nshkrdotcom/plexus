defmodule Plexus.Application do
  @moduledoc """
  Application supervisor for Plexus.

  Global processes are intentionally limited to directories and partitioned
  registries. Hot-path graph, cache, budget and record state is owned per run.
  """
  use Application

  @impl true
  def start(_type, _args) do
    partitions = max(System.schedulers_online(), 1)

    children = [
      {Registry, keys: :unique, name: Plexus.Registry, partitions: partitions},
      Plexus.Run.Directory,
      {DynamicSupervisor, name: Plexus.RunSupervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Plexus.Supervisor)
  end
end
