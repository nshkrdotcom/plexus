defmodule Plexus.Actor.Context do
  @moduledoc """
  Run-scoped context passed to actors through `init_arg`.
  """

  @enforce_keys [:run, :run_id]
  defstruct [:run, :run_id, :actor_id, :parent_id, metadata: %{}]

  @type t :: %__MODULE__{
          run: pid() | atom(),
          run_id: term(),
          actor_id: term() | nil,
          parent_id: term() | nil,
          metadata: map()
        }
end
