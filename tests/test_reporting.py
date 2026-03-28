from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oss_ai_stack_map.config.loader import (
    BenchmarkConfig,
    BenchmarkEntity,
    BenchmarkThresholds,
    ClassificationConfig,
    DiscoveryConfig,
    EnvSettings,
    ExclusionConfig,
    HttpConfig,
    OutputConfig,
    RuntimeConfig,
    SegmentConfig,
    StudyConfig,
    TechnologyAlias,
    TechnologyAliasConfig,
)
from oss_ai_stack_map.pipeline.reporting import (
    build_benchmark_recall_report,
    build_gap_report,
    build_report_summary,
)


def test_build_report_summary_counts_unique_repo_edges(tmp_path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "example/app-1",
                    "description": "Uses OpenAI",
                    "topics": ["llm"],
                    "stars": 100,
                },
                {
                    "repo_id": 2,
                    "full_name": "example/app-2",
                    "description": "Uses LangChain",
                    "topics": ["agent"],
                    "stars": 50,
                },
                {
                    "repo_id": 3,
                    "full_name": "example/app-3",
                    "description": "Not included",
                    "topics": [],
                    "stars": 10,
                },
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "example/app-1",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 2,
                    "full_name": "example/app-2",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 3,
                    "full_name": "example/app-3",
                    "passed_serious_filter": False,
                    "passed_ai_relevance_filter": False,
                    "passed_major_filter": False,
                },
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "technology_id": "openai",
                    "provider_id": "openai",
                },
                {
                    "repo_id": 1,
                    "technology_id": "openai",
                    "provider_id": "openai",
                },
                {
                    "repo_id": 2,
                    "technology_id": "openai",
                    "provider_id": "openai",
                },
                {
                    "repo_id": 2,
                    "technology_id": "langchain",
                    "provider_id": None,
                },
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "technology_id": "openai",
                    "display_name": "OpenAI SDK",
                    "category_id": "model_access_and_providers",
                    "aliases": ["openai"],
                    "repo_names": [],
                    "package_prefixes": [],
                },
                {
                    "technology_id": "langchain",
                    "display_name": "LangChain",
                    "category_id": "orchestration_and_agents",
                    "aliases": ["langchain"],
                    "repo_names": [],
                    "package_prefixes": [],
                },
            ]
        ),
        tmp_path / "technologies.parquet",
    )

    summary = build_report_summary(tmp_path, top_n=5)

    assert summary.total_repos == 3
    assert summary.final_repos == 2
    assert summary.top_technologies[0]["technology_id"] == "openai"
    assert summary.top_technologies[0]["repo_count"] == 2
    assert summary.top_providers[0]["provider_id"] == "openai"
    assert summary.top_providers[0]["repo_count"] == 2
    assert summary.gap_report.final_repos_missing_edges_count == 0


def test_build_report_summary_handles_snapshots_without_edge_table(tmp_path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "example/app-1",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                }
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "example/app-1",
                    "description": "No mapped technologies",
                    "topics": ["llm"],
                    "stars": 100,
                }
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "readme_text": "",
                    "tree_paths": [],
                    "manifest_paths": [],
                    "manifest_dependencies": [],
                    "sbom_dependencies": [],
                    "import_dependencies": [],
                }
            ]
        ),
        tmp_path / "repo_contexts.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "technology_id": "openai",
                    "display_name": "OpenAI",
                    "category_id": "providers",
                }
            ]
        ),
        tmp_path / "technologies.parquet",
    )

    summary = build_report_summary(tmp_path, top_n=5)

    assert summary.final_repos == 1
    assert summary.top_technologies == []
    assert summary.top_providers == []
    assert summary.gap_report.final_repos_missing_edges_count == 1


def test_build_gap_report_surfaces_missing_edges_and_unmatched_prefixes(tmp_path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "description": "The AI SDK",
                    "topics": ["vercel", "llm"],
                    "stars": 1000,
                },
                {
                    "repo_id": 2,
                    "full_name": "cloudflare/partykit",
                    "description": "PartyKit, for Workers",
                    "topics": ["cloudflare", "agents"],
                    "stars": 500,
                },
                {
                    "repo_id": 3,
                    "full_name": "custom/app",
                    "description": "App using niche SDKs",
                    "topics": ["ai"],
                    "stars": 200,
                },
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 2,
                    "full_name": "cloudflare/partykit",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 3,
                    "full_name": "custom/app",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "technology_id": "vercel-ai-sdk",
                    "provider_id": None,
                }
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 3,
                    "package_name": "react",
                    "technology_id": None,
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
                {
                    "repo_id": 3,
                    "package_name": "@types/node",
                    "technology_id": None,
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
                {
                    "repo_id": 3,
                    "package_name": "@unknownco/agent-sdk",
                    "technology_id": None,
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
                {
                    "repo_id": 3,
                    "package_name": "@unknownco/llm-client",
                    "technology_id": None,
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
            ]
        ),
        tmp_path / "repo_dependency_evidence.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "technology_id": "vercel-ai-sdk",
                    "display_name": "Vercel AI SDK",
                    "category_id": "ai_developer_tools_and_sdk_families",
                    "aliases": ["ai-sdk", "vercel ai sdk"],
                    "repo_names": ["vercel/ai"],
                    "package_prefixes": ["@ai-sdk/"],
                },
                {
                    "technology_id": "cloudflare-agents",
                    "display_name": "Cloudflare Agents",
                    "category_id": "runtime_and_agent_deployment",
                    "aliases": ["cloudflare agents"],
                    "repo_names": ["cloudflare/agents"],
                    "package_prefixes": ["workers-ai"],
                },
            ]
        ),
        tmp_path / "technologies.parquet",
    )

    gap_report = build_gap_report(tmp_path, top_n=5)

    assert gap_report.final_repos_missing_edges_count == 2
    assert (
        gap_report.final_repos_missing_edges_with_unmapped_dependency_evidence_count == 1
    )
    assert gap_report.final_repos_missing_edges_with_no_dependency_evidence_count == 1
    assert gap_report.final_repos_missing_edges[0]["full_name"] == "custom/app"
    assert gap_report.final_repos_missing_edges[0]["gap_reason"] == "unmapped_dependency_evidence"
    assert gap_report.top_unmatched_packages[0]["package_name"] == "@unknownco/agent-sdk"
    assert gap_report.top_unmatched_package_prefixes[0]["package_prefix"] == "@unknownco/"
    assert (
        gap_report.top_ai_specific_unmatched_package_prefixes[0]["package_prefix"]
        == "@unknownco/"
    )
    assert not gap_report.top_commodity_unmatched_package_prefixes
    assert any(
        row["entry_type"] == "package_prefix" and row["value"] == "@unknownco/"
        for row in gap_report.suggested_discovery_inputs
    )
    assert all(row["package_name"] != "react" for row in gap_report.top_unmatched_packages)
    assert all(
        row["package_prefix"] != "@types/" for row in gap_report.top_unmatched_package_prefixes
    )
    assert gap_report.top_vendor_like_unmapped_repos[0]["full_name"] == "cloudflare/partykit"


def test_build_benchmark_recall_report_tracks_entity_coverage(tmp_path: Path) -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            classification=ClassificationConfig(),
            outputs=OutputConfig(write_csv=False),
            http=HttpConfig(),
        ),
        discovery=DiscoveryConfig(topics=[], description_keywords=[], manual_seed_repos=[]),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            excluded_directories=[],
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="openai",
                    display_name="OpenAI SDK",
                    category_id="model_access_and_providers",
                    provider_id="openai",
                    aliases=["openai"],
                )
            ]
        ),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["ai-sdk"],
                    package_prefixes=["@ai-sdk/"],
                    repo_names=["vercel/ai"],
                ),
                TechnologyAlias(
                    technology_id="browserbase",
                    display_name="Browserbase",
                    category_id="browser_and_computer_use_infra",
                    entity_type="product",
                    aliases=["browserbase"],
                    package_prefixes=["@browserbasehq/"],
                    repo_names=["browserbase/stagehand"],
                ),
            ]
        ),
        benchmarks=BenchmarkConfig(
            thresholds=BenchmarkThresholds(
                min_repo_discovered_rate=1.0,
                min_repo_included_rate=1.0,
                min_repo_identity_mapped_rate=1.0,
                min_third_party_adoption_rate=1.0,
                min_dependency_evidence_rate=1.0,
                severity="warning",
            ),
            entities=[
                BenchmarkEntity(
                    entity_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    technology_ids=["vercel-ai-sdk", "openai"],
                    repo_names=["vercel/ai"],
                    package_prefixes=["@ai-sdk/"],
                ),
                BenchmarkEntity(
                    entity_id="browserbase",
                    display_name="Browserbase",
                    technology_ids=["browserbase"],
                    repo_names=["browserbase/stagehand"],
                    package_prefixes=["@browserbasehq/"],
                ),
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "description": "AI SDK",
                    "topics": ["vercel"],
                    "stars": 1000,
                },
                {
                    "repo_id": 2,
                    "full_name": "custom/app",
                    "description": "App using Vercel AI SDK",
                    "topics": ["ai"],
                    "stars": 500,
                },
                {
                    "repo_id": 3,
                    "full_name": "browserbase/stagehand",
                    "description": "Browser automation framework",
                    "topics": ["browserbase"],
                    "stars": 750,
                },
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 2,
                    "full_name": "custom/app",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 3,
                    "full_name": "browserbase/stagehand",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": False,
                },
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "technology_id": "vercel-ai-sdk",
                    "provider_id": None,
                    "match_method": "repo_identity",
                },
                {
                    "repo_id": 2,
                    "full_name": "custom/app",
                    "technology_id": "vercel-ai-sdk",
                    "provider_id": None,
                    "match_method": "package_prefix",
                },
                {
                    "repo_id": 2,
                    "full_name": "custom/app",
                    "technology_id": "openai",
                    "provider_id": "openai",
                    "match_method": "derived_provider",
                },
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 2,
                    "package_name": "@ai-sdk/openai",
                    "technology_id": "vercel-ai-sdk",
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
                {
                    "repo_id": 3,
                    "package_name": "@browserbasehq/stagehand",
                    "technology_id": None,
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                },
            ]
        ),
        tmp_path / "repo_dependency_evidence.parquet",
    )

    report = build_benchmark_recall_report(input_dir=tmp_path, runtime=runtime)

    assert report.entity_count == 2
    assert report.entities_with_repo_discovered == 2
    assert report.entities_with_repo_discovered_by_search == 0
    assert report.entities_with_repo_discovered_by_seed_only == 0
    assert report.entities_with_repo_included == 1
    assert report.entities_with_repo_identity_mapped == 1
    assert report.entities_with_third_party_adoption == 1
    assert report.entities_with_dependency_evidence == 2
    assert {row["metric"] for row in report.failed_thresholds} == {
        "repo_included_rate",
        "repo_identity_mapped_rate",
        "third_party_adoption_rate",
    }
    assert report.prioritized_gaps[0]["entity_id"] == "browserbase"
    by_id = {row["entity_id"]: row for row in report.entities}
    assert by_id["vercel-ai-sdk"]["third_party_adoption"] is True
    assert by_id["browserbase"]["dependency_evidence_found"] is True


def test_build_benchmark_recall_report_separates_seeded_vs_search_discovery(tmp_path: Path) -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            classification=ClassificationConfig(),
            outputs=OutputConfig(write_csv=False),
            http=HttpConfig(),
        ),
        discovery=DiscoveryConfig(topics=[], description_keywords=[], manual_seed_repos=[]),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            excluded_directories=[],
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    aliases=["ai-sdk"],
                    repo_names=["vercel/ai"],
                    capabilities=["model_access"],
                ),
            ]
        ),
        benchmarks=BenchmarkConfig(
            entities=[
                BenchmarkEntity(
                    entity_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    technology_ids=["vercel-ai-sdk"],
                    repo_names=["vercel/ai"],
                ),
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "description": "AI SDK",
                    "topics": ["ai"],
                    "stars": 100,
                    "discovery_queries": ["repo:vercel/ai"],
                    "discovery_source_types": ["anchor_seed", "benchmark_seed", "registry_seed"],
                }
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                }
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "vercel/ai",
                    "technology_id": "vercel-ai-sdk",
                    "match_method": "repo_identity",
                }
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 2,
                    "package_name": "@ai-sdk/openai",
                    "technology_id": "vercel-ai-sdk",
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                }
            ]
        ),
        tmp_path / "repo_dependency_evidence.parquet",
    )

    report = build_benchmark_recall_report(input_dir=tmp_path, runtime=runtime)

    assert report.entities_with_repo_discovered == 1
    assert report.entities_with_repo_discovered_by_anchor == 1
    assert report.entities_with_repo_discovered_by_search == 0
    assert report.entities_with_repo_discovered_by_seed_only == 1
    assert report.repo_discovered_rate == 1.0
    assert report.repo_discovered_by_anchor_rate == 1.0
    assert report.repo_discovered_by_search_rate == 0.0
    assert report.repo_discovered_by_seed_only_rate == 1.0
    assert "exact seed" in report.prioritized_gaps[0]["reasons"][0] or any(
        "exact seed" in reason for reason in report.prioritized_gaps[0]["reasons"]
    )


def test_benchmark_prioritization_ignores_self_only_dependency_evidence(tmp_path: Path) -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            classification=ClassificationConfig(),
            outputs=OutputConfig(write_csv=False),
            http=HttpConfig(),
        ),
        discovery=DiscoveryConfig(topics=[], description_keywords=[], manual_seed_repos=[]),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            excluded_directories=[],
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="steel-browser",
                    display_name="Steel Browser",
                    category_id="browser_and_computer_use_infra",
                    entity_type="product",
                    aliases=["steel-browser"],
                    package_prefixes=["@steel-browser/"],
                    repo_names=["steel-dev/steel-browser"],
                ),
            ]
        ),
        benchmarks=BenchmarkConfig(
            entities=[
                BenchmarkEntity(
                    entity_id="steel-browser",
                    display_name="Steel Browser",
                    technology_ids=["steel-browser"],
                    repo_names=["steel-dev/steel-browser"],
                    package_prefixes=["@steel-browser/"],
                ),
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "steel-dev/steel-browser",
                    "description": "Browser automation",
                    "topics": ["browser"],
                    "stars": 100,
                }
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "steel-dev/steel-browser",
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                }
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "full_name": "steel-dev/steel-browser",
                    "technology_id": "steel-browser",
                    "match_method": "repo_identity",
                }
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "package_name": "@steel-browser/api",
                    "technology_id": "steel-browser",
                    "source_path": "package.json",
                    "evidence_type": "manifest",
                }
            ]
        ),
        tmp_path / "repo_dependency_evidence.parquet",
    )

    report = build_benchmark_recall_report(input_dir=tmp_path, runtime=runtime)

    assert report.entity_count == 1
    assert report.entities[0]["third_party_dependency_evidence_found"] is False
    assert report.prioritized_gaps[0]["entity_id"] == "steel-browser"
    assert any("exact seed" in reason for reason in report.prioritized_gaps[0]["reasons"])
