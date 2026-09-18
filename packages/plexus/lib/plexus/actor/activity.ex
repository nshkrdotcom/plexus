defmodule Plexus.Actor.Activity do
  @moduledoc false
  alias Plexus.Run.Config
  alias Plexus.Schedule.Quiescence

  def begin(run_id, actor_id) do
    config = Config.fetch!(run_id)
    ticket = make_ref()
    Quiescence.add(config.quiescence, :messages, 1)
    :ets.insert(config.tables.activity, {ticket, actor_id})
    {run_id, ticket}
  end

  def active?({run_id, ticket}) do
    case Config.fetch(run_id) do
      {:ok, config} -> :ets.member(config.tables.activity, ticket)
      _ -> false
    end
  end

  def finish({run_id, ticket}) do
    case Config.fetch(run_id) do
      {:ok, config} ->
        case :ets.take(config.tables.activity, ticket) do
          [] -> :ok
          [_] -> Quiescence.add(config.quiescence, :messages, -1)
        end

      _ ->
        :ok
    end
  end

  def cancel_actor(run_id, actor_id) do
    config = Config.fetch!(run_id)

    for {ticket, ^actor_id} <- :ets.match_object(config.tables.activity, {:_, actor_id}),
        do: finish({run_id, ticket})

    :ok
  end
end
