defmodule Plexus.Application do
  @moduledoc """
  Application supervisor for Plexus.

  Starts the run supervisor, actor registry, and graph metadata store.
  """
  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Plexus.Registry},
      {DynamicSupervisor, name: Plexus.RunSupervisor, strategy: :one_for_one},
      Plexus.Graph
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Plexus.Supervisor)
  end
end
