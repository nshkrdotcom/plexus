Code.require_file("../support/http.exs", __DIR__)
Code.require_file("../support/data.exs", __DIR__)
Code.require_file("../support/runtime.exs", __DIR__)

alias Plexus.Examples.Support.{Data, HTTP, Runtime}

{opts, _, _} = OptionParser.parse(System.argv(), strict: [data_dir: :string])
data_dir = opts[:data_dir] || Runtime.data_dir("swe-bench-verified")
path = Path.join(data_dir, "swe_bench_verified.jsonl")
File.mkdir_p!(data_dir)

base = "https://datasets-server.huggingface.co/rows"
page_size = 100

fetch_page = fn offset ->
  HTTP.query(base, %{
    "dataset" => "SWE-bench/SWE-bench_Verified",
    "config" => "default",
    "split" => "test",
    "offset" => offset,
    "length" => page_size
  })
  |> HTTP.get_json!()
end

first = fetch_page.(0)
total = first["num_rows_total"] || length(first["rows"] || [])

rows =
  Stream.iterate(0, &(&1 + page_size))
  |> Stream.take_while(&(&1 < total))
  |> Enum.flat_map(fn
    0 -> first["rows"] || []
    offset -> fetch_page.(offset)["rows"] || []
  end)
  |> Enum.map(&Map.fetch!(&1, "row"))
  |> Enum.take(total)

Data.write_jsonl!(path, rows)
IO.puts("Wrote #{length(rows)} SWE-bench Verified instances to #{path}")
