defmodule Plexus.Telemetry do
  @moduledoc """
  Plexus lifecycle telemetry and privacy-preserving forwarding of TypeSafe events.
  """

  alias Plexus.{Budget, Record}
  alias Plexus.Run.Config

  @typesafe_events [
    [:typesafe_sdk, :evaluate, :start],
    [:typesafe_sdk, :evaluate, :stop],
    [:typesafe_sdk, :evaluate, :exception],
    [:typesafe_sdk, :answer],
    [:typesafe_sdk, :batch, :cancelled]
  ]

  @spec emit(term(), [atom()], map(), map()) :: :ok
  def emit(run_id, suffix, measurements \\ %{}, metadata \\ %{}) do
    :telemetry.execute([:plexus | suffix], measurements, Map.put(metadata, :run_id, run_id))
    :ok
  end

  @spec attach_typesafe(term()) :: :ok
  def attach_typesafe(run_id) do
    handler_id = handler_id(run_id)

    case :telemetry.attach_many(
           handler_id,
           @typesafe_events,
           &__MODULE__.handle_typesafe/4,
           run_id
         ) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  @spec detach_typesafe(term()) :: :ok
  def detach_typesafe(run_id) do
    :telemetry.detach(handler_id(run_id))
    :ok
  end

  @doc false
  def handle_typesafe(event, measurements, metadata, run_id) do
    caller = Map.get(metadata, :caller, %{})

    if Map.get(caller, :plexus_run_id) == run_id do
      safe_metadata =
        metadata
        |> Map.drop([:caller, :telemetry_span_context])
        |> Map.put(:actor_id, Map.get(caller, :plexus_actor_id))
        |> Map.put(:class, Map.get(caller, :plexus_class))

      maybe_record_tokens(run_id, event, measurements)

      Record.append(run_id, {:typesafe, List.to_tuple(event)}, %{
        measurements: measurements,
        metadata: safe_metadata
      })
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp maybe_record_tokens(run_id, [:typesafe_sdk, :evaluate, :stop], measurements) do
    tokens = Map.get(measurements, :input_tokens, 0) + Map.get(measurements, :output_tokens, 0)
    Budget.consume(Config.fetch!(run_id).budget, :tokens, tokens)
  end

  defp maybe_record_tokens(_run_id, _event, _measurements), do: :ok

  defp handler_id(run_id), do: {__MODULE__, run_id}
end
