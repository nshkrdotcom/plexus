defmodule Plexus.Schedule do
  @moduledoc """
  Run-level execution regime.

  `:async` interprets effects immediately. `{:bsp, opts}` buffers actor effects
  until `barrier/1`, allowing the same actor implementation to be replayed under
  bulk-synchronous execution. `{:bounded_async, k}` permits each actor at most
  k command envelopes ahead of the least advanced active actor. Idle active
  actors deliberately hold back faster peers; completion removes a participant.
  A command envelope is one update regardless of its number of effects.
  Priority mode orders the currently pending envelopes by the supplied key;
  it cannot order against arrivals that have not happened yet.
  """

  alias Plexus.Actor.{Activity, Interpreter}
  alias Plexus.Run.{Config, Names}
  alias Plexus.Schedule.Quiescence

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
      :async -> dispatch_async(run_id, envelope)
      _ -> GenServer.cast(Names.schedule_server(run_id), {:dispatch, envelope})
    end

    :ok
  end

  defp dispatch_async(run_id, envelope) do
    own_callback? = Plexus.Registry.lookup(run_id, envelope.context.actor_id) == {:ok, self()}

    destructive? =
      Enum.any?(envelope.commands, fn
        {:prune, _} -> true
        {:population, _} -> true
        _ -> false
      end)

    if own_callback? and destructive?,
      do: GenServer.cast(Names.schedule_server(run_id), {:dispatch, envelope}),
      else: Interpreter.execute_now(run_id, envelope)
  end

  @doc false
  def sleep(run_id, actor_id, timeout) do
    config = Config.fetch!(run_id)
    token = make_ref()
    Quiescence.add(config.quiescence, :timers, 1)
    :ets.insert(config.tables.timers, {token, actor_id, nil})

    ref =
      Process.send_after(GenServer.whereis(config.schedule_server), {:wake_actor, token}, timeout)

    :ets.update_element(config.tables.timers, token, {3, ref})
    :ok
  end

  @doc false
  def cancel_actor(run_id, actor_id) do
    config = Config.fetch!(run_id)

    for {token, ^actor_id, _ref} <- :ets.match_object(config.tables.timers, {:_, actor_id, :_}) do
      cancel_timer(config, token)
    end

    Activity.cancel_actor(run_id, actor_id)
  end

  defp cancel_timer(config, token) do
    case :ets.take(config.tables.timers, token) do
      [{_, _, ref}] ->
        if is_reference(ref), do: Process.cancel_timer(ref)
        Quiescence.add(config.quiescence, :timers, -1)

      [] ->
        :ok
    end
  end

  @spec barrier(term()) :: {:ok, non_neg_integer(), non_neg_integer()}
  def barrier(run_id), do: GenServer.call(Names.schedule_server(run_id), :barrier, :infinity)

  @spec set_regime(term(), regime()) :: :ok
  def set_regime(run_id, regime) do
    regime = normalize(regime)
    GenServer.call(Names.schedule_server(run_id), {:set_regime, regime})
  end
end
