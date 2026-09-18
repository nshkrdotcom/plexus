defmodule Plexus.Actor.Command do
  @moduledoc """
  Declarative effects emitted by semantic actors.

  Effects pass through `Plexus.Actor.Interpreter`, which is the single seam for
  admission, batching, scheduling, topology, pruning and run recording.
  """

  @type t ::
          {:spawn, atom(), module(), map(), keyword()}
          | {:edge, atom(), term(), term(), number()}
          | {:edge, atom(), term(), term(), number(), term()}
          | {:send, term(), term()}
          | {:measure, term(), term(), term(), keyword()}
          | {:expand, term(), term(), keyword()}
          | {:belief, term()}
          | {:budget, :reserve | :refund, :measure | :expand | :tokens | :population,
             non_neg_integer()}
          | {:prune, term()}
          | {:sleep, timeout()}
          | {:wake_on, term()}
          | {:complete, term()}
end
