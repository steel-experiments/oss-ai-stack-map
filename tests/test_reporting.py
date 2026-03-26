from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oss_ai_stack_map.pipeline.reporting import build_report_summary


def test_build_report_summary_counts_unique_repo_edges(tmp_path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "repo_id": 1,
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 2,
                    "passed_serious_filter": True,
                    "passed_ai_relevance_filter": True,
                    "passed_major_filter": True,
                },
                {
                    "repo_id": 3,
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
                },
                {
                    "technology_id": "langchain",
                    "display_name": "LangChain",
                    "category_id": "orchestration_and_agents",
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
