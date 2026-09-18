defmodule Plexus.Examples.EvidenceWorker do
  @moduledoc "Example leaf actor using the framework-managed measurement path."

  use Plexus.Actor

  alias Plexus.Actor
  alias TypeSafeSDK.Response

  @impl true
  def init(%{text: text} = args) do
    prepared =
      TypeSafeSDK.prepare!(
        relevance:
          TypeSafeSDK.score("How relevant is this evidence to the root issue?", [
            "low",
            "medium",
            "high"
          ]),
        actionable: TypeSafeSDK.noul("Is this evidence actionable?")
      )

    {:ok,
     %{
       text: text,
       context: Actor.context(args),
       prepared: prepared,
       result: nil
     }}
  end

  @impl true
  def handle_cast(:analyze, state) do
    :ok =
      Actor.dispatch(
        state.context,
        {:measure, :evidence, %{text: state.text}, state.prepared, []}
      )

    {:noreply, state}
  end

  def handle_cast({:plexus, :measurement, :evidence, {:ok, response}}, state) do
    result = %{
      response: response,
      relevance: Response.fetch(response, :relevance),
      actionable: Response.fetch(response, :actionable)
    }

    {:noreply, %{state | result: result}}
  end

  def handle_cast({:plexus, :measurement, :evidence, {:error, error}}, state) do
    {:noreply, %{state | result: {:error, error}}}
  end

  @impl true
  def handle_call(:result, _from, state), do: {:reply, state.result, state}

  # Low-level TypeSafe OTP evaluations remain available as an escape hatch.
  @impl true
  def handle_evaluation(result, tag, state) do
    {:noreply, Map.put(state, :last_direct_evaluation, {tag, result})}
  end
end
