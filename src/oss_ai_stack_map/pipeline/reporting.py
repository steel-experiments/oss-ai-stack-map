from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass
class ReportSummary:
    total_repos: int
    serious_repos: int
    ai_relevant_repos: int
    final_repos: int
    top_technologies: list[dict]
    top_providers: list[dict]


def build_report_summary(input_dir: Path, top_n: int = 10) -> ReportSummary:
    decisions = pq.read_table(
        input_dir / "repo_inclusion_decisions.parquet",
        columns=["passed_serious_filter", "passed_ai_relevance_filter", "passed_major_filter"],
    ).to_pylist()
    edges = pq.read_table(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "technology_id", "provider_id"],
    ).to_pylist()
    technologies = {
        row["technology_id"]: row
        for row in pq.read_table(
            input_dir / "technologies.parquet",
            columns=["technology_id", "display_name", "category_id"],
        ).to_pylist()
    }

    total_repos = len(decisions)
    serious_repos = sum(1 for row in decisions if row["passed_serious_filter"])
    ai_relevant_repos = sum(1 for row in decisions if row["passed_ai_relevance_filter"])
    final_repos = sum(1 for row in decisions if row["passed_major_filter"])

    tech_counter: Counter[str] = Counter()
    provider_counter: Counter[str] = Counter()
    seen_pairs: set[tuple[int, str]] = set()
    seen_provider_pairs: set[tuple[int, str]] = set()
    for edge in edges:
        tech_pair = (edge["repo_id"], edge["technology_id"])
        if tech_pair not in seen_pairs:
            tech_counter[edge["technology_id"]] += 1
            seen_pairs.add(tech_pair)
        provider_id = edge.get("provider_id")
        if provider_id:
            provider_pair = (edge["repo_id"], provider_id)
            if provider_pair not in seen_provider_pairs:
                provider_counter[provider_id] += 1
                seen_provider_pairs.add(provider_pair)

    top_technologies = []
    for technology_id, repo_count in tech_counter.most_common(top_n):
        tech = technologies.get(technology_id, {})
        share = repo_count / final_repos if final_repos else 0.0
        top_technologies.append(
            {
                "technology_id": technology_id,
                "display_name": tech.get("display_name", technology_id),
                "category_id": tech.get("category_id"),
                "repo_count": repo_count,
                "repo_share": round(share, 4),
            }
        )

    top_providers = []
    for provider_id, repo_count in provider_counter.most_common(top_n):
        share = repo_count / final_repos if final_repos else 0.0
        top_providers.append(
            {
                "provider_id": provider_id,
                "repo_count": repo_count,
                "repo_share": round(share, 4),
            }
        )

    return ReportSummary(
        total_repos=total_repos,
        serious_repos=serious_repos,
        ai_relevant_repos=ai_relevant_repos,
        final_repos=final_repos,
        top_technologies=top_technologies,
        top_providers=top_providers,
    )
