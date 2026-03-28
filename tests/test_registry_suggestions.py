from __future__ import annotations

import json
from pathlib import Path

from oss_ai_stack_map.config.loader import BenchmarkConfig, TechnologyAlias, TechnologyAliasConfig
from oss_ai_stack_map.pipeline.registry_suggestions import build_registry_suggestion_report


def test_build_registry_suggestion_report_skips_known_families(runtime_config, tmp_path: Path) -> None:
    runtime = runtime_config["runtime"]
    runtime.benchmarks = BenchmarkConfig(entities=[])
    runtime.aliases.technologies = [
        TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            provider_id="openai",
            aliases=["openai"],
        )
    ]
    runtime.registry = TechnologyAliasConfig(technologies=[])

    payload = {
        "candidate_count": 3,
        "graph_node_count": 3,
        "graph_edge_count": 2,
        "unmatched_package_count": 10,
        "unmatched_repo_count": 5,
        "top_candidates": [
            {
                "family_id": "openai",
                "display_name": "Openai",
                "priority_score": 50.0,
                "final_repo_count": 10,
                "missing_edge_repo_count": 1,
                "benchmark_overlap_count": 1,
                "scoped_package_count": 2,
                "example_packages": ["openai-whisper", "@azure/openai"],
                "example_repos": ["openai/codex"],
                "suggested_repo_names": ["openai/codex"],
            },
            {
                "family_id": "workflow",
                "display_name": "Workflow",
                "priority_score": 28.0,
                "final_repo_count": 13,
                "missing_edge_repo_count": 1,
                "benchmark_overlap_count": 0,
                "scoped_package_count": 3,
                "example_packages": ["@llamaindex/workflow", "@temporalio/workflow"],
                "example_repos": ["n8n-io/n8n"],
                "suggested_repo_names": ["llm-workflow-engine/llm-workflow-engine"],
            },
            {
                "family_id": "prompt",
                "display_name": "Prompt",
                "priority_score": 40.0,
                "final_repo_count": 20,
                "missing_edge_repo_count": 2,
                "benchmark_overlap_count": 0,
                "scoped_package_count": 1,
                "example_packages": ["prompt-toolkit", "prompt_toolkit"],
                "example_repos": ["f/prompts.chat"],
                "suggested_repo_names": ["f/prompts.chat"],
            },
            {
                "family_id": "rag",
                "display_name": "RAG",
                "priority_score": 31.0,
                "final_repo_count": 3,
                "missing_edge_repo_count": 1,
                "benchmark_overlap_count": 0,
                "scoped_package_count": 1,
                "example_packages": ["@runanywhere/rag", "ragas"],
                "example_repos": ["infiniflow/ragflow"],
                "suggested_repo_names": ["infiniflow/ragflow"],
            },
        ],
    }
    (tmp_path / "technology_discovery_report.json").write_text(json.dumps(payload), encoding="utf-8")

    report = build_registry_suggestion_report(
        input_dir=tmp_path,
        runtime=runtime,
        top_n=10,
    )

    assert report.suggestion_count == 1
    by_id = {row["candidate_family_id"]: row for row in report.suggestions}
    assert "openai" not in by_id
    assert "workflow" not in by_id
    assert "prompt" not in by_id
    assert by_id["rag"]["suggested_category_id"] == "orchestration_and_agents"
    assert "@runanywhere/rag" in by_id["rag"]["suggested_package_prefixes"]
    assert by_id["rag"]["suggested_entity_id"] == "runanywhere-rag"
    assert by_id["rag"]["suggested_display_name"] == "Runanywhere/RAG"
    assert by_id["rag"]["confidence"] == "medium"
