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
          "guides/kernel-primitives.md",
          "guides/actor-runtime.md",
          "guides/typesafe-integration.md",
          "guides/graph-and-subtrees.md",
          "guides/calibration-and-replay.md",
          "guides/expansion.md",
          "guides/testing-and-release.md",
          "guides/examples.md",
          "examples/README.md",
          "examples/DATASETS.md"
        ] do
      assert File.exists?(path), "missing expected file: #{path}"
    end
  end

  test "example applications are packaged, ignored safely, and have entrypoints" do
    package_files = Mix.Project.config()[:package][:files]
    assert "examples" in package_files

    gitignore = File.read!(".gitignore")
    assert gitignore =~ "/.plexus-data/"
    assert gitignore =~ "/examples/**/data/"
    assert gitignore =~ "/examples/**/.data/"
    assert gitignore =~ "/examples/**/cache/"
    assert gitignore =~ "/examples/**/downloads/"

    for slug <- [
          "00_issue_swarm",
          "01_city_signal_tracker",
          "02_incident_commander",
          "03_dependency_upgrade_search",
          "04_research_evidence_graph",
          "05_alert_swarm"
        ],
        entry <- ["fetch.exs", "run.exs"] do
      assert File.exists?(Path.join(["examples", slug, entry])),
             "missing example entrypoint: #{slug}/#{entry}"
    end
  end
end