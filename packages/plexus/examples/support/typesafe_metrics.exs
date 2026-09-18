defmodule Plexus.Examples.Support.TypeSafeMetrics do
  @moduledoc """
  Process-local live TypeSafe transport evidence for the standalone examples.

  The collector records privacy-safe TypeSafe SDK telemetry only: endpoint/model
  configuration, response status, request id, model, token usage, retry count,
  and Plexus run/contract identifiers already present in SDK telemetry.

  It never records API keys, semantic states, prompts, request bodies, response
  bodies, or authorization headers.
  """

  @name __MODULE__
  @handler_id {__MODULE__, :collector}
  @max_request_records 200

  @events [
    [:typesafe_sdk, :evaluate, :start],
    [:typesafe_sdk, :evaluate, :stop],
    [:typesafe_sdk, :evaluate, :exception]
  ]

  def ensure_started(opts \\ []) do
    case Process.whereis(@name) do
      nil ->
        {:ok, _pid} = Agent.start(fn -> initial_state() end, name: @name)
        attach!()

        if Keyword.get(opts, :print_at_exit, true) do
          System.at_exit(fn _status ->
            print_summary()
          end)
        end

        :ok

      _pid ->
        :ok
    end
  end

  def register_client(client) do
    ensure_started()

    info = %{
      base_url: Map.get(client, :base_url),
      requested_model: Map.get(client, :default_model),
      transport: client |> Map.get(:transport) |> inspect()
    }

    Agent.update(@name, fn state ->
      %{state | clients: [info | state.clients] |> Enum.uniq()}
    end)

    :ok
  end

  def reset! do
    ensure_started(print_at_exit: false)
    Agent.update(@name, fn _ -> initial_state() end)
    :ok
  end

  def snapshot do
    ensure_started(print_at_exit: false)

    Agent.get(@name, fn state ->
      state
      |> Map.update!(:requests, &Enum.reverse/1)
      |> Map.update!(:runs, &MapSet.to_list/1)
      |> Map.update!(:contracts, &MapSet.to_list/1)
      |> Map.update!(:clients, &Enum.reverse/1)
    end)
  end

  def handle(event, measurements, metadata, agent_name) do
    case Process.whereis(agent_name) do
      pid when is_pid(pid) ->
        Agent.update(pid, &update_event(&1, event, measurements, metadata))

      _ ->
        :ok
    end
  end

  def print_summary do
    case Process.whereis(@name) do
      nil ->
        :ok

      _pid ->
        summary = snapshot()

        IO.puts("")
        IO.puts("===== TYPESAFE LIVE TRANSPORT SUMMARY =====")

        case summary.clients do
          [] ->
            IO.puts("client endpoint               <not registered>")
            IO.puts("client requested model        <not registered>")
            IO.puts("client transport              <not registered>")

          clients ->
            Enum.each(clients, fn client ->
              IO.puts("client endpoint               #{client.base_url || "<default>"}")
              IO.puts("client requested model        #{client.requested_model || "<default>"}")
              IO.puts("client transport              #{client.transport}")
            end)
        end

        IO.puts("TypeSafe evaluate starts       #{summary.starts}")
        IO.puts("TypeSafe evaluate stops        #{summary.stops}")
        IO.puts("TypeSafe exceptions            #{summary.exceptions}")

        IO.puts("confirmed HTTP responses      #{summary.confirmed_http_responses}")

        IO.puts("confirmed HTTP 2xx            #{summary.http_successes}")

        IO.puts("confirmed HTTP non-2xx        #{summary.http_errors}")

        IO.puts("minimum physical HTTP calls   #{summary.confirmed_http_responses}")

        IO.puts("reported retries               #{summary.retries}")
        IO.puts("TypeSafe input tokens          #{summary.input_tokens}")
        IO.puts("TypeSafe output tokens         #{summary.output_tokens}")

        IO.puts("TypeSafe total tokens          #{summary.input_tokens + summary.output_tokens}")

        IO.puts("distinct Plexus runs           #{MapSet.size(MapSet.new(summary.runs))}")

        IO.puts("distinct contract fingerprints #{MapSet.size(MapSet.new(summary.contracts))}")

        IO.puts("statuses                       #{format_counts(summary.statuses)}")
        IO.puts("models                         #{format_counts(summary.models)}")

        IO.puts(
          "request IDs captured           #{length(summary.requests)}/#{summary.confirmed_http_responses}"
        )

        Enum.each(summary.requests, fn request ->
          IO.puts(
            "  #{request.request_id} status=#{request.status} " <>
              "model=#{request.model || "<unknown>"} " <>
              "input=#{request.input_tokens} output=#{request.output_tokens} " <>
              "retries=#{request.retries}"
          )
        end)

        IO.puts("===== END TYPESAFE LIVE TRANSPORT SUMMARY =====")
        :ok
    end
  rescue
    _ ->
      :ok
  end

  defp attach! do
    case :telemetry.attach_many(
           @handler_id,
           @events,
           &__MODULE__.handle/4,
           @name
         ) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  defp initial_state do
    %{
      starts: 0,
      stops: 0,
      exceptions: 0,
      confirmed_http_responses: 0,
      http_successes: 0,
      http_errors: 0,
      retries: 0,
      input_tokens: 0,
      output_tokens: 0,
      statuses: %{},
      models: %{},
      runs: MapSet.new(),
      contracts: MapSet.new(),
      requests: [],
      clients: []
    }
  end

  defp update_event(
         state,
         [:typesafe_sdk, :evaluate, :start],
         _measurements,
         metadata
       ) do
    state
    |> Map.update!(:starts, &(&1 + 1))
    |> record_caller(metadata)
  end

  defp update_event(
         state,
         [:typesafe_sdk, :evaluate, :exception],
         _measurements,
         metadata
       ) do
    state
    |> Map.update!(:exceptions, &(&1 + 1))
    |> record_caller(metadata)
  end

  defp update_event(
         state,
         [:typesafe_sdk, :evaluate, :stop],
         measurements,
         metadata
       ) do
    status = metadata[:status]
    request_id = metadata[:request_id]
    model = metadata[:model]
    retries = integer(metadata[:retries])
    input_tokens = integer(measurements[:input_tokens])
    output_tokens = integer(measurements[:output_tokens])

    confirmed? =
      is_integer(status) and
        is_binary(request_id) and
        request_id != ""

    success? = confirmed? and status >= 200 and status < 300

    request = %{
      request_id: request_id,
      status: status,
      model: model,
      input_tokens: input_tokens,
      output_tokens: output_tokens,
      retries: retries
    }

    state
    |> Map.update!(:stops, &(&1 + 1))
    |> Map.update!(:retries, &(&1 + retries))
    |> Map.update!(:input_tokens, &(&1 + input_tokens))
    |> Map.update!(:output_tokens, &(&1 + output_tokens))
    |> maybe_increment(:confirmed_http_responses, confirmed?)
    |> maybe_increment(:http_successes, success?)
    |> maybe_increment(:http_errors, confirmed? and not success?)
    |> increment_count(:statuses, status)
    |> increment_count(:models, model)
    |> record_caller(metadata)
    |> maybe_record_request(request, confirmed?)
  end

  defp update_event(state, _event, _measurements, _metadata), do: state

  defp record_caller(state, metadata) do
    caller = metadata[:caller] || %{}

    state
    |> maybe_put_set(:runs, caller[:plexus_run_id])
    |> maybe_put_set(:contracts, caller[:plexus_contract_fingerprint])
  end

  defp maybe_put_set(state, _field, nil), do: state

  defp maybe_put_set(state, field, value) do
    Map.update!(state, field, &MapSet.put(&1, inspect(value)))
  end

  defp maybe_increment(state, _field, false), do: state
  defp maybe_increment(state, field, true), do: Map.update!(state, field, &(&1 + 1))

  defp increment_count(state, _field, nil), do: state

  defp increment_count(state, field, value) do
    Map.update!(state, field, fn counts ->
      Map.update(counts, value, 1, &(&1 + 1))
    end)
  end

  defp maybe_record_request(state, _request, false), do: state

  defp maybe_record_request(state, request, true) do
    requests =
      [request | state.requests]
      |> Enum.take(@max_request_records)

    %{state | requests: requests}
  end

  defp integer(value) when is_integer(value), do: value
  defp integer(_), do: 0

  defp format_counts(counts) when map_size(counts) == 0, do: "<none>"

  defp format_counts(counts) do
    counts
    |> Enum.sort_by(fn {key, _count} -> inspect(key) end)
    |> Enum.map_join(", ", fn {key, count} -> "#{key}=#{count}" end)
  end
end
