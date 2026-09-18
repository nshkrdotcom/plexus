defmodule Plexus.Graph do
  @moduledoc """
  Lightweight in-memory subtree metadata store.

  The graph is intentionally small and single-node. It tracks parent/child
  relationships and module/metadata labels so a run can inspect or prune its
  actor population.
  """

  use GenServer

  @table :plexus_graph

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @spec put(term(), term(), keyword()) :: :ok
  def put(run_id, actor_id, attrs) do
    GenServer.call(__MODULE__, {:put, run_id, actor_id, Enum.into(attrs, %{})})
  end

  @spec get(term(), term()) :: map() | nil
  def get(run_id, actor_id) do
    case :ets.lookup(@table, {run_id, actor_id}) do
      [{{^run_id, ^actor_id}, attrs}] -> attrs
      [] -> nil
    end
  end

  @spec attach_child(term(), term(), term()) :: :ok
  def attach_child(run_id, parent_id, child_id),
    do: GenServer.call(__MODULE__, {:attach, run_id, parent_id, child_id})

  @spec children(term(), term()) :: [term()]
  def children(run_id, actor_id) do
    get(run_id, actor_id)
    |> case do
      nil -> []
      attrs -> Map.get(attrs, :children, [])
    end
  end

  @spec subtree(term(), term()) :: [term()]
  def subtree(run_id, actor_id),
    do: do_subtree(run_id, actor_id, MapSet.new()) |> MapSet.to_list()

  @spec prune(term(), term()) :: :ok
  def prune(run_id, actor_id), do: GenServer.call(__MODULE__, {:prune, run_id, actor_id})

  defp do_subtree(run_id, actor_id, visited) do
    if MapSet.member?(visited, actor_id) do
      visited
    else
      visited = MapSet.put(visited, actor_id)

      Enum.reduce(children(run_id, actor_id), visited, fn child, acc ->
        do_subtree(run_id, child, acc)
      end)
    end
  end

  @impl true
  def handle_call({:put, run_id, actor_id, attrs}, _from, state) do
    existing = get(run_id, actor_id) || %{}
    merged = Map.merge(%{children: []}, existing) |> Map.merge(attrs)
    :ets.insert(@table, {{run_id, actor_id}, merged})
    {:reply, :ok, state}
  end

  def handle_call({:attach, run_id, parent_id, child_id}, _from, state) do
    parent = Map.merge(%{children: []}, get(run_id, parent_id) || %{})
    child = Map.merge(%{children: []}, get(run_id, child_id) || %{})

    :ets.insert(
      @table,
      {{run_id, parent_id}, %{parent | children: Enum.uniq(parent.children ++ [child_id])}}
    )

    :ets.insert(@table, {{run_id, child_id}, Map.put(child, :parent, parent_id)})
    {:reply, :ok, state}
  end

  def handle_call({:prune, run_id, actor_id}, _from, state) do
    for id <- subtree(run_id, actor_id), do: :ets.delete(@table, {run_id, id})
    {:reply, :ok, state}
  end
end
