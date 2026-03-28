from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oss_ai_stack_map.config.loader import (
    BenchmarkConfig,
    BenchmarkEntity,
    TechnologyAlias,
    TechnologyAliasConfig,
)
from oss_ai_stack_map.pipeline.technology_discovery import build_technology_discovery_report


def test_build_technology_discovery_report_ranks_candidate_families(runtime_config, tmp_path: Path) -> None:
    runtime = runtime_config["runtime"]
    runtime.benchmarks = BenchmarkConfig(
        entities=[
            BenchmarkEntity(
                entity_id="mastra",
                display_name="Mastra",
                repo_names=["mastra-ai/mastra"],
                package_prefixes=["@mastra/"],
            )
        ]
    )
    runtime.registry = TechnologyAliasConfig(
        technologies=[
            TechnologyAlias(
                technology_id="known-tech",
                display_name="Known Tech",
                category_id="orchestration_and_agents",
                aliases=["known-tech"],
                package_prefixes=["known-tech"],
            )
        ]
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                {"repo_id": 1, "full_name": "custom/app-1", "stars": 1000},
                {"repo_id": 2, "full_name": "custom/app-2", "stars": 800},
                {"repo_id": 3, "full_name": "custom/app-3", "stars": 600},
            ]
        ),
        tmp_path / "repos.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"repo_id": 1, "passed_major_filter": True},
                {"repo_id": 2, "passed_major_filter": True},
                {"repo_id": 3, "passed_major_filter": False},
            ]
        ),
        tmp_path / "repo_inclusion_decisions.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"repo_id": 2, "technology_id": "known-tech"},
            ]
        ),
        tmp_path / "repo_technology_edges.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"repo_id": 1, "package_name": "@mastra/core", "technology_id": None},
                {"repo_id": 1, "package_name": "@pydantic/logfire-api", "technology_id": None},
                {"repo_id": 2, "package_name": "@mastra/mcp", "technology_id": None},
                {"repo_id": 2, "package_name": "typescript", "technology_id": None},
                {"repo_id": 3, "package_name": "@mastra/schema", "technology_id": None},
            ]
        ),
        tmp_path / "repo_dependency_evidence.parquet",
    )

    report = build_technology_discovery_report(input_dir=tmp_path, runtime=runtime, top_n=10)

    assert report.candidate_count == 2
    assert report.graph_node_count == 2
    assert report.top_candidates[0]["family_id"] == "mastra"
    by_id = {row["family_id"]: row for row in report.top_candidates}
    assert by_id["mastra"]["benchmark_overlap_count"] >= 1
    assert by_id["mastra"]["final_repo_count"] == 2
    assert by_id["mastra"]["missing_edge_repo_count"] == 1
    assert "typescript" not in by_id
