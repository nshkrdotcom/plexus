defmodule Plexus.ReleaseConsistencyTest do
  use ExUnit.Case, async: true

  test "release metadata stays aligned" do
    assert Mix.Project.config()[:app] == :plexus
    assert Mix.Project.config()[:version] == "0.1.0"
    assert Mix.Project.config()[:docs][:source_ref] == "v0.1.0"
    assert File.read!("CHANGELOG.md") =~ "## [0.1.0] - 2026-09-17"
    assert File.read!("README.md") =~ ~s({:plexus, "~> 0.1.0"})
  end

  test "all packaged extras and brand assets exist" do
    for path <- [
          "README.md",
          "CHANGELOG.md",
          "HANDOFF.md",
          "LICENSE",
          "assets/plexus.svg",
          "guides/index.md",
          "guides/getting-started.md",
          "guides/architecture.md",
          "guides/actor-runtime.md",
          "guides/typesafe-integration.md",
          "guides/graph-and-subtrees.md",
          "guides/testing-and-release.md"
        ] do
      assert File.exists?(path), "missing expected file: #{path}"
    end
  end
end
