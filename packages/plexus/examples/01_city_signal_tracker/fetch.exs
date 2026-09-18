Code.require_file("../support/http.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.{Data, HTTP, Runtime}

{opts, _, _} =
  OptionParser.parse(Runtime.cli_args(),
    strict: [data_dir: :string, limit: :integer, days: :integer]
  )

data_dir = opts[:data_dir] || Runtime.data_dir("nyc-311")
limit = opts[:limit] || 20_000
days = opts[:days] || 7
if limit < 1, do: raise(ArgumentError, "--limit must be at least 1")
if days < 1, do: raise(ArgumentError, "--days must be at least 1")
since = Date.add(Date.utc_today(), -days) |> Date.to_iso8601()

until =
  DateTime.utc_now()
  |> DateTime.to_naive()
  |> NaiveDateTime.truncate(:second)
  |> NaiveDateTime.to_iso8601()

path = Path.join(data_dir, "requests.jsonl")
File.mkdir_p!(data_dir)

headers =
  case System.get_env("SOCRATA_APP_TOKEN") do
    nil -> []
    "" -> []
    token -> [{"X-App-Token", token}]
  end

page_size = 5_000
pages = div(limit + page_size - 1, page_size)

rows =
  0..(pages - 1)
  |> Stream.flat_map(fn page ->
    remaining = limit - page * page_size
    take = min(page_size, remaining)

    params = %{
      "$select" =>
        "unique_key,created_date,agency,complaint_type,descriptor,location_type,borough,latitude,longitude,status",
      "$where" =>
        "created_date >= '#{since}T00:00:00.000' AND created_date <= '#{until}' AND latitude IS NOT NULL AND longitude IS NOT NULL",
      "$order" => "created_date DESC, unique_key DESC",
      "$limit" => take,
      "$offset" => page * page_size
    }

    url = HTTP.query("https://data.cityofnewyork.us/resource/erm2-nwe9.json", params)
    HTTP.get_json!(url, headers)
  end)
  |> Stream.take(limit)

Data.write_jsonl!(path, rows)
IO.puts("Wrote up to #{limit} NYC 311 records from the last #{days} days to #{path}")
