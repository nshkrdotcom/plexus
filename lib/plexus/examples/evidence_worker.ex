defmodule Plexus.Examples.EvidenceWorker do
  @moduledoc """
  Example leaf actor.

  Each worker scores one evidence item against a prepared question set.
  """

  use Plexus.Actor

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
       context: Map.take(args, [:run, :run_id, :actor_id, :parent_id]),
       prepared: prepared,
       result: nil
     }}
  end

  @impl true
  def handle_cast(:analyze, state) do
    {:evaluate, {:evidence, %{text: state.text}, state.prepared}, state}
  end

  @impl true
  def handle_call(:result, _from, state) do
    {:reply, state.result, state}
  end

  @impl true
  def handle_evaluation({:ok, response}, :evidence, state) do
    result = %{
      response: response,
      relevance: Response.fetch(response, :relevance),
      actionable: Response.fetch(response, :actionable)
    }

    {:noreply, %{state | result: result}}
  end

  def handle_evaluation({:error, error}, :evidence, state) do
    {:noreply, %{state | result: {:error, error}}}
  end
end
