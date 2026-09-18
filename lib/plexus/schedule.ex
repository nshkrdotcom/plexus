defmodule Plexus.Schedule do
  @moduledoc """
  Run-level execution regime.

  `:async` interprets effects immediately. `{:bsp, opts}` buffers actor effects
  until `barrier/1`, allowing the same actor implementation to be replayed under
  bulk-synchronous execution. Bounded-async and priority regimes use the policy
  server as an ordering seam and are intentionally conservative in this first
  implementation.
  """

  alias Plexus.Actor.{Activity, Interpreter}
  alias Plexus.Run.{Config, Names}

  @type regime ::
          :async | {:bsp, keyword()} | {:bounded_async, pos_integer()} | {:priority, function()}

  @spec normalize(term()) :: regime()
  def normalize(:async), do: :async
  def normalize({:bsp, opts}) when is_list(opts), do: {:bsp, opts}
  def normalize({:bounded_async, k}) when is_integer(k) and k > 0, do: {:bounded_async, k}
  def normalize({:priority, fun}) when is_function(fun, 1), do: {:priority, fun}

  def normalize(other),
    do: raise(ArgumentError, "invalid Plexus scheduling regime: #{inspect(other)}")

  @spec dispatch(term(), map()) :: :ok
  def dispatch(run_id, envelope) do
    ticket = Activity.begin(run_id, envelope.context.actor_id)
    envelope = Map.put(envelope, :activity, ticket)

    case Config.fetch!(run_id).schedule do
      :async -> Interpreter.execute_now(run_id, envelope)
      _ -> GenServer.cast(Names.schedule_server(run_id), {:dispatch, envelope})
    end

    :ok
  end

  @spec barrier(term()) :: {:ok, non_neg_integer(), non_neg_integer()}
  def barrier(run_id), do: GenServer.call(Names.schedule_server(run_id), :barrier, :infinity)

  @spec set_regime(term(), regime()) :: :ok
  def set_regime(run_id, regime) do
    regime = normalize(regime)
    :ok = Config.update(run_id, &Map.put(&1, :schedule, regime))
    GenServer.call(Names.schedule_server(run_id), {:set_regime, regime})
  end
end
