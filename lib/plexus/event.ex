defmodule Plexus.Event do
  @moduledoc "Run-local one-shot wake subscriptions used by `{:wake_on, event}` commands."

  alias Plexus.{Record, Run}
  alias Plexus.Run.Config

  @spec subscribe(term(), term(), term()) :: :ok
  def subscribe(run_id, event, actor_id) do
    config = Config.fetch!(run_id)
    :ets.insert(config.tables.waiters, {event, actor_id})
    :ok
  end

  @spec publish(term(), term(), term()) :: non_neg_integer()
  def publish(run_id, event, payload \\ nil) do
    config = Config.fetch!(run_id)
    waiters = :ets.lookup(config.tables.waiters, event)
    :ets.delete(config.tables.waiters, event)

    Enum.each(waiters, fn {^event, actor_id} ->
      Run.cast(run_id, actor_id, {:plexus, :event, event, payload})
    end)

    count = length(waiters)
    Record.append(run_id, :event_publish, %{subscriber_count: count})
    count
  end
end
