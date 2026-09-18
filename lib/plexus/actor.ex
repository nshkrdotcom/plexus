defmodule Plexus.Actor do
  @moduledoc """
  Thin ergonomics layer for semantic actors.

  `use Plexus.Actor` also uses `TypeSafeSDK.OTP.Server`. The calling module still
  implements the normal callbacks expected by `TypeSafeSDK.OTP.Server`.
  """

  alias Plexus.Actor.Context

  defmacro __using__(_opts) do
    quote do
      use TypeSafeSDK.OTP.Server

      def cast(pid, message), do: GenServer.cast(pid, message)
      def call(pid, message, timeout \\ 5_000), do: GenServer.call(pid, message, timeout)

      defoverridable cast: 2, call: 3
    end
  end

  @spec context(keyword()) :: Context.t()
  def context(opts) do
    %Context{
      run: Keyword.fetch!(opts, :run),
      run_id: Keyword.fetch!(opts, :run_id),
      actor_id: Keyword.get(opts, :actor_id),
      parent_id: Keyword.get(opts, :parent_id),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end
end
