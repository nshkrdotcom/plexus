defmodule Plexus.Actor.Command do
  @moduledoc """
  Lightweight coordination commands emitted by semantic actors.
  """

  @type t ::
          {:spawn, module(), keyword()}
          | {:send, term(), term()}
          | {:link_child, term(), term()}
          | {:prune, term()}
          | {:complete, term()}
end
