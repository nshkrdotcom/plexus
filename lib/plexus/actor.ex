defmodule Plexus.Actor do
  @moduledoc """
  Ergonomics for semantic actors.

  `use Plexus.Actor` keeps `TypeSafeSDK.OTP.Server` as a low-level escape hatch.
  Framework-managed effects should be sent through `dispatch/2`, so Plexus can
  interpose budgets, batching, topology, scheduling and cancellation.
  """

  alias Plexus.Actor.{Context, Interpreter}

  defmacro __using__(_opts) do
    quote do
      use TypeSafeSDK.OTP.Server

      def cast(pid, message), do: GenServer.cast(pid, message)
      def call(pid, message, timeout \\ 5_000), do: GenServer.call(pid, message, timeout)

      defoverridable cast: 2, call: 3
    end
  end

  @spec context(keyword() | map()) :: Context.t()
  def context(opts) when is_list(opts) do
    %Context{
      run: Keyword.fetch!(opts, :run),
      run_id: Keyword.fetch!(opts, :run_id),
      actor_id: Keyword.get(opts, :actor_id),
      parent_id: Keyword.get(opts, :parent_id),
      class: Keyword.get(opts, :class),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  def context(%{} = opts) do
    %Context{
      run: Map.fetch!(opts, :run),
      run_id: Map.fetch!(opts, :run_id),
      actor_id: Map.get(opts, :actor_id),
      parent_id: Map.get(opts, :parent_id),
      class: Map.get(opts, :class),
      metadata: Map.get(opts, :metadata, %{})
    }
  end

  @spec dispatch(Context.t() | map(), Plexus.Actor.Command.t() | [Plexus.Actor.Command.t()]) ::
          :ok
  def dispatch(context, commands), do: Interpreter.dispatch(context, commands)
end
