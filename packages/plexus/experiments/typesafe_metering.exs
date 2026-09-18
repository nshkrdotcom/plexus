defmodule Plexus.Experiment.TypeSafeMetering do
  @moduledoc false

  def run(argv) do
    args =
      case argv do
        ["--" | rest] -> rest
        other -> other
      end

    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          count: :integer,
          timeout_ms: :integer,
          base_url: :string,
          model: :string
        ]
      )

    if rest != [] or invalid != [] do
      raise "invalid arguments: rest=#{inspect(rest)} invalid=#{inspect(invalid)}"
    end

    count = opts[:count] || 10
    timeout_ms = opts[:timeout_ms] || 30_000

    if count < 1 do
      raise "--count must be positive"
    end

    api_key = System.fetch_env!("TYPESAFE_API_KEY")

    client_opts = [
      api_key: api_key,
      retry: false,
      timeout_ms: timeout_ms
    ]

    client_opts =
      maybe_put(
        client_opts,
        :base_url,
        opts[:base_url] || System.get_env("TYPESAFE_BASE_URL")
      )

    client_opts =
      maybe_put(
        client_opts,
        :model,
        opts[:model] ||
          System.get_env("TYPESAFE_MODEL") ||
          System.get_env("TYPESAFE_DEFAULT_MODEL")
      )

    client = TypeSafeSDK.new_client(client_opts)

    prepared =
      TypeSafeSDK.prepare!(
        metering_probe:
          TypeSafeSDK.noul(
            "Is this state explicitly identified as a TypeSafe API metering probe?"
          )
      )

    run_nonce =
      "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

    IO.puts("===== TYPESAFE METERING PROBE =====")
    IO.puts("endpoint                    #{client.base_url}")
    IO.puts("requested model             #{client.default_model}")
    IO.puts("transport                   #{inspect(client.transport)}")
    IO.puts("requests planned            #{count}")
    IO.puts("run nonce                   #{run_nonce}")
    IO.puts("retries                     disabled")

    started = System.monotonic_time(:millisecond)

    results =
      Enum.map(1..count, fn index ->
        nonce = "#{run_nonce}-#{index}"

        result =
          TypeSafeSDK.evaluate(
            client,
            %{
              purpose: "typesafe-metering-probe",
              nonce: nonce,
              sequence: index
            },
            prepared,
            telemetry_metadata: %{
              experiment: "typesafe-metering",
              run_nonce: run_nonce,
              sequence: index
            }
          )

        case result do
          {:ok, response} ->
            status =
              case response.raw_http_response do
                %{status: value} -> value
                _ -> nil
              end

            row = %{
              "sequence" => index,
              "nonce" => nonce,
              "status" => status,
              "request_id" => response.request_id,
              "model" => response.model,
              "input_tokens" => response.usage.input_tokens || 0,
              "output_tokens" => response.usage.output_tokens || 0,
              "retries" => response.retries || 0,
              "elapsed_ms" => response.elapsed_ms
            }

            IO.puts(
              "request #{index}/#{count} " <>
                "status=#{status} " <>
                "request_id=#{response.request_id} " <>
                "model=#{response.model} " <>
                "input=#{row["input_tokens"]} " <>
                "output=#{row["output_tokens"]}"
            )

            row

          {:error, error} ->
            row = %{
              "sequence" => index,
              "nonce" => nonce,
              "error" => inspect(error)
            }

            IO.puts("request #{index}/#{count} ERROR #{inspect(error)}")
            row
        end
      end)

    elapsed_ms = System.monotonic_time(:millisecond) - started

    successful =
      Enum.filter(results, fn row ->
        status = row["status"]

        is_integer(status) and
          status >= 200 and
          status < 300 and
          is_binary(row["request_id"]) and
          row["request_id"] != ""
      end)

    request_ids =
      successful
      |> Enum.map(& &1["request_id"])

    unique_request_ids = MapSet.new(request_ids)

    input_tokens =
      successful
      |> Enum.map(&(&1["input_tokens"] || 0))
      |> Enum.sum()

    output_tokens =
      successful
      |> Enum.map(&(&1["output_tokens"] || 0))
      |> Enum.sum()

    total_tokens = input_tokens + output_tokens

    baseline_requests = env_integer("TYPESAFE_DASHBOARD_BASE_REQUESTS")
    baseline_tokens = env_integer("TYPESAFE_DASHBOARD_BASE_TOKENS")

    dashboard_expectation = %{
      "baseline_requests" => baseline_requests,
      "baseline_tokens" => baseline_tokens,
      "expected_requests_after_ingestion" =>
        add_if_present(baseline_requests, length(successful)),
      "expected_tokens_after_ingestion" => add_if_present(baseline_tokens, total_tokens)
    }

    report = %{
      "run_nonce" => run_nonce,
      "endpoint" => client.base_url,
      "requested_model" => client.default_model,
      "transport" => inspect(client.transport),
      "planned_requests" => count,
      "successful_requests" => length(successful),
      "unique_request_ids" => MapSet.size(unique_request_ids),
      "input_tokens" => input_tokens,
      "output_tokens" => output_tokens,
      "total_tokens" => total_tokens,
      "elapsed_ms" => elapsed_ms,
      "dashboard_expectation" => dashboard_expectation,
      "requests" => results
    }

    report_dir = Path.join(System.tmp_dir!(), "plexus-typesafe-metering")
    File.mkdir_p!(report_dir)
    path = Path.join(report_dir, "#{run_nonce}.json")
    File.write!(path, Jason.encode!(report, pretty: true))

    IO.puts("")
    IO.puts("===== METERING RESULT =====")
    IO.puts("successful requests          #{length(successful)}")
    IO.puts("unique request IDs           #{MapSet.size(unique_request_ids)}")
    IO.puts("input tokens                 #{input_tokens}")
    IO.puts("output tokens                #{output_tokens}")
    IO.puts("total tokens                 #{total_tokens}")
    IO.puts("elapsed ms                   #{elapsed_ms}")
    IO.puts("report                       #{path}")

    if baseline_requests do
      IO.puts("dashboard baseline requests  #{baseline_requests}")

      IO.puts("expected requests after lag #{baseline_requests + length(successful)}")
    end

    if baseline_tokens do
      IO.puts("dashboard baseline tokens    #{baseline_tokens}")
      IO.puts("expected tokens after lag    #{baseline_tokens + total_tokens}")
    end

    if length(successful) != count do
      raise "only #{length(successful)}/#{count} requests succeeded with confirmed HTTP responses"
    end

    if MapSet.size(unique_request_ids) != count do
      raise "expected #{count} unique request ids, got #{MapSet.size(unique_request_ids)}"
    end

    :ok
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, _key, ""), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp env_integer(name) do
    case System.get_env(name) do
      nil ->
        nil

      "" ->
        nil

      value ->
        case Integer.parse(value) do
          {number, ""} -> number
          _ -> raise "#{name} must be an integer"
        end
    end
  end

  defp add_if_present(nil, _delta), do: nil
  defp add_if_present(value, delta), do: value + delta
end

Plexus.Experiment.TypeSafeMetering.run(System.argv())
