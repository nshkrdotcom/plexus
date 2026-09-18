defmodule Plexus.Examples.IncidentCommander.Topology do
  @moduledoc false

  alias Plexus.Graph

  defstruct [:spans, :waiting_children, :seen_edges, :hypothesis_count]

  def new do
    %__MODULE__{
      spans: table(:set),
      waiting_children: table(:bag),
      seen_edges: table(:set),
      hypothesis_count: :atomics.new(1, signed: false)
    }
  end

  def close(%__MODULE__{} = topology) do
    Enum.each([topology.spans, topology.waiting_children, topology.seen_edges], fn table ->
      try do
        :ets.delete(table)
      rescue
        ArgumentError -> :ok
      end
    end)

    :ok
  end

  def reserve_hypothesis(%__MODULE__{hypothesis_count: counter}, max_hypotheses)
      when is_integer(max_hypotheses) and max_hypotheses > 0 do
    reserve_counter(counter, max_hypotheses)
  end

  def release_hypothesis(%__MODULE__{hypothesis_count: counter}) do
    release_counter(counter)
  end

  def hypothesis_count(%__MODULE__{hypothesis_count: counter}), do: :atomics.get(counter, 1)

  def observe_trace(_run_id, _topology, _service, %{source: source}) when source != :trace, do: []

  def observe_trace(run_id, %__MODULE__{} = topology, service, event) do
    row = event.raw
    trace_id = row["trace_id"]
    span_id = row["span_id"]
    parent_id = row["parent_id"]

    if blank?(trace_id) or blank?(span_id) do
      []
    else
      span_key = {trace_id, span_id}
      true = :ets.insert(topology.spans, {span_key, service})

      from_waiting =
        topology.waiting_children
        |> :ets.lookup(span_key)
        |> Enum.flat_map(fn {^span_key, child_service} ->
          connect(run_id, topology, service, child_service, event.event_id)
        end)

      :ets.delete(topology.waiting_children, span_key)

      to_parent =
        if blank?(parent_id) do
          []
        else
          parent_key = {trace_id, parent_id}

          case :ets.lookup(topology.spans, parent_key) do
            [{^parent_key, caller_service}] ->
              connect(run_id, topology, caller_service, service, event.event_id)

            [] ->
              :ets.insert(topology.waiting_children, {parent_key, service})
              []
          end
        end

      Enum.uniq(from_waiting ++ to_parent)
    end
  end

  def neighbors(run_id, service, :callers) do
    run_id
    |> Graph.incoming({:service, service}, :calls)
    |> Enum.flat_map(&service_from_edge/1)
    |> Enum.uniq()
  end

  def neighbors(run_id, service, :callees) do
    run_id
    |> Graph.outgoing({:service, service}, :calls)
    |> Enum.flat_map(&service_from_edge/1)
    |> Enum.uniq()
  end

  def neighbors(run_id, service, :both) do
    Enum.uniq(neighbors(run_id, service, :callers) ++ neighbors(run_id, service, :callees))
  end

  defp connect(_run_id, _topology, service, service, _event_id), do: []

  defp connect(run_id, topology, caller, callee, event_id) do
    key = {caller, callee}

    if :ets.insert_new(topology.seen_edges, {key}) do
      Graph.add_edge(
        run_id,
        :calls,
        {:service, caller},
        {:service, callee},
        1.0,
        %{first_evidence_event: event_id}
      )

      [{caller, callee}]
    else
      []
    end
  end

  defp service_from_edge(%{node: {:service, service}}) when is_binary(service), do: [service]
  defp service_from_edge(_), do: []

  defp table(kind) do
    :ets.new(:plexus_gaia_live, [
      kind,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])
  end

  defp blank?(value), do: value in [nil, ""]

  defp reserve_counter(counter, limit) do
    current = :atomics.get(counter, 1)

    cond do
      current >= limit ->
        {:error, :hypothesis_limit}

      :atomics.compare_exchange(counter, 1, current, current + 1) == :ok ->
        :ok

      true ->
        reserve_counter(counter, limit)
    end
  end

  defp release_counter(counter) do
    current = :atomics.get(counter, 1)

    cond do
      current <= 0 -> :ok
      :atomics.compare_exchange(counter, 1, current, current - 1) == :ok -> :ok
      true -> release_counter(counter)
    end
  end
end
