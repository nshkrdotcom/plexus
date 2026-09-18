Code.require_file("../support/http.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.{Data, HTTP, Runtime}

{opts, _, _} = OptionParser.parse(Runtime.cli_args(), strict: [data_dir: :string, year: :integer])
year = opts[:year] || Date.utc_today().year - 1
data_dir = opts[:data_dir] || Runtime.data_dir("noaa-storm-events")
File.mkdir_p!(data_dir)

index_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
index = HTTP.get!(index_url)
pattern = ~r/StormEvents_details-ftp_v1\.0_d#{year}_c\d+\.csv\.gz/

filename =
  Regex.scan(pattern, index)
  |> List.flatten()
  |> Enum.uniq()
  |> Enum.sort()
  |> List.last() || raise("NOAA index has no Storm Events details file for #{year}")

gz = Path.join(data_dir, filename)
csv = Path.join(data_dir, "storm_events_#{year}.csv")
HTTP.download!(index_url <> filename, gz)
Data.gunzip!(gz, csv)
File.write!(Path.join(data_dir, "active_year.txt"), Integer.to_string(year))

IO.puts("NOAA Storm Events #{year} available at #{csv}")
