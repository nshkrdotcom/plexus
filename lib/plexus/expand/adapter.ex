defmodule Plexus.Expand.Adapter do
  @moduledoc """
  Minimal provider-neutral seam for the expensive expansion tier.

  Plexus deliberately does not own provider transport. An integration adapter
  receives the run's inference client and returns `{:ok, response}` or
  `{:error, reason}`. Capability maps must use explicit `:supported` values;
  missing/unknown capabilities fail closed.
  """

  @callback capabilities(client :: term()) :: map()
  @callback expand(client :: term(), spec :: term(), opts :: keyword()) :: {:ok, term()} | {:error, term()}
  @optional_callbacks capabilities: 1
end
