from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import pyarrow.parquet as pq

from oss_ai_stack_map.config.loader import RuntimeConfig

GENERIC_SCOPED_BASES = {
    "agent",
    "agents",
    "api",
    "browser",
    "chat",
    "client",
    "clients",
    "core",
    "mcp",
    "provider",
    "providers",
    "runtime",
    "schema",
    "sdk",
    "server",
    "servers",
    "tool",
    "tools",
    "ui",
    "worker",
    "workers",
}
GENERIC_FAMILY_IDS = {
    "atomic",
    "azure",
    "async",
    "cli",
    "default",
    "dev",
    "embedded",
    "entity",
    "eslint",
    "fastapi",
    "fixed",
    "free",
    "github",
    "google",
    "global",
    "i18next",
    "jest",
    "lang",
    "language",
    "linux",
    "logger",
    "match",
    "microsoft",
    "minimap",
    "model",
    "node",
    "open",
    "opentelemetry",
    "pydantic",
    "plugin",
    "playground",
    "prettier",
    "preview",
    "react",
    "reactive",
    "rust",
    "sass",
    "space",
    "python",
    "requests",
    "rollup",
    "serde",
    "tailwind",
    "tailwindcss",
    "tsconfig",
    "typescript",
    "universal",
    "vite",
    "vitest",
    "vscode",
    "web",
}
GENERIC_PREFIX_PREFIXES = (
    "@babel/",
    "@img/",
    "@rollup/",
    "@types/",
)
AI_HINT_EXACT_TOKENS = {
    "ai",
    "browser",
    "cuda",
    "huggingface",
    "llm",
    "logfire",
    "mcp",
    "openai",
    "qdrant",
    "rag",
    "sandbox",
    "stagehand",
    "tokenizer",
    "tokenizers",
    "torch",
    "vector",
    "vllm",
}
AI_HINT_PREFIX_TOKENS = (
    "agent",
    "anthropic",
    "chat",
    "copilot",
    "dataset",
    "diffus",
    "embed",
    "eval",
    "guard",
    "inference",
    "lang",
    "model",
    "ollama",
    "prompt",
    "serve",
    "token",
    "transform",
)


@dataclass
class TechnologyDiscoveryReport:
    candidate_count: int
    graph_node_count: int
    graph_edge_count: int
    unmatched_package_count: int
    unmatched_repo_count: int
    top_candidates: list[dict[str, Any]]


def build_technology_discovery_report(
    *,
    input_dir: Path,
    runtime: RuntimeConfig | None = None,
    top_n: int = 25,
) -> TechnologyDiscoveryReport:
    repos = _read_if_exists(
        input_dir / "repos.parquet",
        columns=["repo_id", "full_name", "stars"],
    )
    repo_by_id = {row["repo_id"]: row for row in repos}
    repo_stars_by_name = {
        row["full_name"]: int(row.get("stars", 0) or 0)
        for row in repos
        if row.get("full_name")
    }
    decisions = _read_if_exists(
        input_dir / "repo_inclusion_decisions.parquet",
        columns=["repo_id", "passed_major_filter"],
    )
    final_repo_ids = {row["repo_id"] for row in decisions if row.get("passed_major_filter")}
    edge_repo_ids = {
        row["repo_id"]
        for row in _read_if_exists(input_dir / "repo_technology_edges.parquet", columns=["repo_id"])
    }
    missing_edge_repo_ids = final_repo_ids - edge_repo_ids
    evidence_rows = _read_if_exists(
        input_dir / "repo_dependency_evidence.parquet",
        columns=["repo_id", "package_name", "technology_id"],
    )
    evidence_purls = _read_optional_column(
        input_dir / "repo_dependency_evidence.parquet",
        columns=["repo_id", "package_name", "purl"],
    )
    purl_by_row_key = {
        (row.get("repo_id"), row.get("package_name")): row.get("purl")
        for row in evidence_purls
    }

    family_to_packages: defaultdict[str, Counter[str]] = defaultdict(Counter)
    family_to_repos: defaultdict[str, set[int]] = defaultdict(set)
    family_to_final_repos: defaultdict[str, set[int]] = defaultdict(set)
    family_to_missing_edge_repos: defaultdict[str, set[int]] = defaultdict(set)
    family_to_star_sum: defaultdict[str, int] = defaultdict(int)
    family_to_repo_examples: defaultdict[str, set[str]] = defaultdict(set)
    family_to_scoped_package_count: defaultdict[str, int] = defaultdict(int)
    family_to_ai_hint_occurrences: defaultdict[str, int] = defaultdict(int)
    repo_to_families: defaultdict[int, set[str]] = defaultdict(set)

    unmatched_package_count = 0
    unmatched_repo_ids: set[int] = set()
    for row in evidence_rows:
        if row.get("technology_id"):
            continue
        package_name = row.get("package_name")
        repo_id = row.get("repo_id")
        if not package_name or repo_id is None:
            continue
        purl = purl_by_row_key.get((repo_id, package_name))
        if should_ignore_discovery_package(package_name=package_name, purl=purl):
            continue
        unmatched_package_count += 1
        unmatched_repo_ids.add(repo_id)
        family_id = infer_candidate_family_id(package_name)
        if is_low_signal_family(family_id):
            continue
        family_to_packages[family_id][package_name] += 1
        if package_name.startswith("@") or "/" in package_name:
            family_to_scoped_package_count[family_id] += 1
        if package_has_ai_affinity(package_name):
            family_to_ai_hint_occurrences[family_id] += 1
        if repo_id not in family_to_repos[family_id]:
            family_to_repos[family_id].add(repo_id)
            family_to_star_sum[family_id] += int(repo_by_id.get(repo_id, {}).get("stars", 0) or 0)
        if repo_id in final_repo_ids:
            family_to_final_repos[family_id].add(repo_id)
        if repo_id in missing_edge_repo_ids:
            family_to_missing_edge_repos[family_id].add(repo_id)
        repo_to_families[repo_id].add(family_id)
        full_name = repo_by_id.get(repo_id, {}).get("full_name")
        if full_name:
            family_to_repo_examples[family_id].add(full_name)

    benchmark_overlap = build_benchmark_overlap_lookup(runtime=runtime)
    candidate_family_ids = {
        family_id
        for family_id in family_to_repos
        if (
            benchmark_overlap.get(family_id, 0) > 0
            or (
                family_to_missing_edge_repos[family_id]
                and generic_family_penalty(family_id) < 0.6
                and family_has_ai_affinity(
                    family_id=family_id,
                    package_counter=family_to_packages[family_id],
                    ai_hint_occurrences=family_to_ai_hint_occurrences[family_id],
                )
            )
        )
    }

    retained_family_ids = select_retained_families(
        candidate_family_ids=candidate_family_ids,
        family_to_repos=family_to_repos,
        family_to_final_repos=family_to_final_repos,
        family_to_missing_edge_repos=family_to_missing_edge_repos,
        family_to_star_sum=family_to_star_sum,
        benchmark_overlap=benchmark_overlap,
        top_n=top_n,
    )

    graph = nx.Graph()
    for family_id in retained_family_ids:
        repo_ids = family_to_repos[family_id]
        graph.add_node(
            family_id,
            repo_count=len(repo_ids),
            final_repo_count=len(family_to_final_repos[family_id]),
            missing_edge_repo_count=len(family_to_missing_edge_repos[family_id]),
            star_sum=family_to_star_sum[family_id],
        )

    for repo_id, families in repo_to_families.items():
        if repo_id not in missing_edge_repo_ids:
            continue
        ordered = sorted(family_id for family_id in families if family_id in retained_family_ids)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                if graph.has_edge(left, right):
                    graph[left][right]["weight"] += 1
                else:
                    graph.add_edge(left, right, weight=1)

    degree_centrality = nx.degree_centrality(graph) if graph.number_of_nodes() > 1 else {}
    if graph.number_of_edges() > 0:
        distance_graph = nx.Graph()
        distance_graph.add_nodes_from(graph.nodes(data=True))
        for left, right, data in graph.edges(data=True):
            distance_graph.add_edge(left, right, distance=1.0 / float(data["weight"]))
        betweenness = nx.betweenness_centrality(distance_graph, weight="distance", normalized=True)
        try:
            eigenvector = nx.eigenvector_centrality(graph, max_iter=1000, weight="weight")
        except nx.NetworkXException:
            eigenvector = {}
    else:
        betweenness = {}
        eigenvector = {}

    weighted_degree = {
        family_id: float(graph.degree(family_id, weight="weight"))
        for family_id in graph.nodes
    }
    metrics_by_family: dict[str, dict[str, float]] = {}
    for family_id in retained_family_ids:
        metrics_by_family[family_id] = {
            "repo_count": float(len(family_to_repos[family_id])),
            "final_repo_count": float(len(family_to_final_repos[family_id])),
            "missing_edge_repo_count": float(len(family_to_missing_edge_repos[family_id])),
            "star_sum": float(family_to_star_sum[family_id]),
            "weighted_degree": weighted_degree.get(family_id, 0.0),
            "degree_centrality": degree_centrality.get(family_id, 0.0),
            "betweenness_centrality": betweenness.get(family_id, 0.0),
            "eigenvector_centrality": eigenvector.get(family_id, 0.0),
            "benchmark_overlap_count": float(benchmark_overlap.get(family_id, 0)),
        }

    score_components = normalize_metric_columns(metrics_by_family)
    ranked_candidates: list[dict[str, Any]] = []
    for family_id, metrics in metrics_by_family.items():
        generic_penalty = generic_family_penalty(family_id)
        priority_score = 100 * (
            0.22 * score_components[family_id]["final_repo_count"]
            + 0.20 * score_components[family_id]["missing_edge_repo_count"]
            + 0.14 * score_components[family_id]["repo_count"]
            + 0.12 * score_components[family_id]["star_sum"]
            + 0.12 * score_components[family_id]["eigenvector_centrality"]
            + 0.10 * score_components[family_id]["betweenness_centrality"]
            + 0.05 * score_components[family_id]["weighted_degree"]
            + 0.05 * score_components[family_id]["benchmark_overlap_count"]
        )
        priority_score *= max(0.05, 1.0 - generic_penalty)

        example_packages = [
            package_name
            for package_name, _ in family_to_packages[family_id].most_common(5)
        ]
        example_repos = sorted(
            family_to_repo_examples[family_id],
            key=lambda full_name: (
                -repo_stars_by_name.get(full_name, 0),
                full_name,
            ),
        )[:5]
        suggested_repo_names = suggest_repo_names(family_id=family_id, repos=repos)
        anchor_penalty = anchor_penalty_multiplier(
            family_id=family_id,
            scoped_package_count=family_to_scoped_package_count[family_id],
            benchmark_overlap_count=int(metrics["benchmark_overlap_count"]),
            suggested_repo_names=suggested_repo_names,
        )
        priority_score *= anchor_penalty
        ranked_candidates.append(
            {
                "family_id": family_id,
                "display_name": display_name_for_family(family_id),
                "priority_score": round(priority_score, 2),
                "repo_count": int(metrics["repo_count"]),
                "final_repo_count": int(metrics["final_repo_count"]),
                "missing_edge_repo_count": int(metrics["missing_edge_repo_count"]),
                "star_sum": int(metrics["star_sum"]),
                "weighted_degree": round(metrics["weighted_degree"], 3),
                "degree_centrality": round(metrics["degree_centrality"], 6),
                "betweenness_centrality": round(metrics["betweenness_centrality"], 6),
                "eigenvector_centrality": round(metrics["eigenvector_centrality"], 6),
                "benchmark_overlap_count": int(metrics["benchmark_overlap_count"]),
                "scoped_package_count": family_to_scoped_package_count[family_id],
                "generic_penalty": round(generic_penalty, 2),
                "example_packages": example_packages,
                "example_repos": example_repos,
                "suggested_repo_names": suggested_repo_names,
            }
        )
    ranked_candidates.sort(
        key=lambda row: (
            -row["priority_score"],
            -row["missing_edge_repo_count"],
            -row["final_repo_count"],
            row["family_id"],
        )
    )

    return TechnologyDiscoveryReport(
        candidate_count=len(candidate_family_ids),
        graph_node_count=graph.number_of_nodes(),
        graph_edge_count=graph.number_of_edges(),
        unmatched_package_count=unmatched_package_count,
        unmatched_repo_count=len(unmatched_repo_ids),
        top_candidates=ranked_candidates[:top_n],
    )


def _read_if_exists(path: Path, columns: list[str] | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pq.read_table(path, columns=columns).to_pylist()


def _read_optional_column(path: Path, columns: list[str]) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    schema_names = set(pq.read_schema(path).names)
    available_columns = [column for column in columns if column in schema_names]
    if not available_columns:
        return []
    return pq.read_table(path, columns=available_columns).to_pylist()


def infer_candidate_family_id(package_name: str) -> str:
    normalized = package_name.casefold().strip()
    if normalized.startswith("@") and "/" in normalized:
        scope, remainder = normalized[1:].split("/", 1)
        base = re.split(r"[-_.]", remainder, maxsplit=1)[0]
        normalized_scope = normalize_scope_token(scope)
        if base and base not in GENERIC_SCOPED_BASES and len(base) >= 3:
            return base
        return normalized_scope
    return re.split(r"[-_.]", normalized, maxsplit=1)[0]


def normalize_scope_token(scope: str) -> str:
    normalized = scope.casefold()
    normalized = re.sub(r"(hq|inc|labs|dev)$", "", normalized)
    return normalized or scope.casefold()


def is_low_signal_family(family_id: str) -> bool:
    return len(family_id) < 3 or family_id in {"js", "ts", "py"}


def generic_family_penalty(family_id: str) -> float:
    normalized = family_id.casefold()
    penalty = 0.0
    if normalized in GENERIC_FAMILY_IDS:
        penalty += 0.65
    if normalized.startswith("github"):
        penalty += 0.25
    if any(normalized.startswith(prefix) for prefix in GENERIC_PREFIX_PREFIXES):
        penalty += 0.65
    return min(penalty, 0.85)


def anchor_penalty_multiplier(
    *,
    family_id: str,
    scoped_package_count: int,
    benchmark_overlap_count: int,
    suggested_repo_names: list[str],
) -> float:
    if scoped_package_count > 0 or benchmark_overlap_count > 0 or suggested_repo_names:
        return 1.0
    if family_id in GENERIC_FAMILY_IDS:
        return 0.2
    return 0.35


def family_has_ai_affinity(
    *,
    family_id: str,
    package_counter: Counter[str],
    ai_hint_occurrences: int,
) -> bool:
    total_occurrences = sum(package_counter.values())
    if total_occurrences <= 0:
        return False
    if package_has_ai_affinity(family_id):
        return True
    ratio = ai_hint_occurrences / total_occurrences
    return ai_hint_occurrences >= 2 and ratio >= 0.15


def package_has_ai_affinity(value: str) -> bool:
    haystacks = [value.casefold()]
    tokens: set[str] = set()
    for haystack in haystacks:
        tokens.update(token for token in re.split(r"[^a-z0-9]+", haystack) if token)
    return any(token in AI_HINT_EXACT_TOKENS for token in tokens) or any(
        token.startswith(prefix)
        for token in tokens
        for prefix in AI_HINT_PREFIX_TOKENS
    )


def should_ignore_discovery_package(*, package_name: str, purl: str | None) -> bool:
    normalized = package_name.casefold()
    if purl and purl.casefold().startswith("pkg:githubactions/"):
        return True
    if normalized.startswith(GENERIC_PREFIX_PREFIXES):
        return True
    return normalized in {"react", "react-dom", "eslint"}


def select_retained_families(
    *,
    candidate_family_ids: set[str],
    family_to_repos: dict[str, set[int]],
    family_to_final_repos: dict[str, set[int]],
    family_to_missing_edge_repos: dict[str, set[int]],
    family_to_star_sum: dict[str, int],
    benchmark_overlap: dict[str, int],
    top_n: int,
) -> set[str]:
    retained_limit = max(250, top_n * 25)
    scored = []
    for family_id in candidate_family_ids:
        repo_ids = family_to_repos[family_id]
        generic_penalty = generic_family_penalty(family_id)
        coarse_score = (
            5.0 * len(family_to_missing_edge_repos[family_id])
            + 4.0 * len(family_to_final_repos[family_id])
            + 2.0 * len(repo_ids)
            + 0.5 * math.log1p(float(family_to_star_sum[family_id]))
            + 2.0 * float(benchmark_overlap.get(family_id, 0))
        ) * max(0.05, 1.0 - generic_penalty)
        scored.append((coarse_score, family_id))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return {family_id for _, family_id in scored[:retained_limit]}


def normalize_metric_columns(metrics_by_family: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    normalized: dict[str, dict[str, float]] = {
        family_id: {} for family_id in metrics_by_family
    }
    metric_names = {name for metrics in metrics_by_family.values() for name in metrics}
    for metric_name in metric_names:
        values = [metrics[metric_name] for metrics in metrics_by_family.values()]
        max_value = max(values) if values else 0.0
        for family_id, metrics in metrics_by_family.items():
            value = metrics[metric_name]
            normalized[family_id][metric_name] = 0.0 if max_value <= 0 else value / max_value
    return normalized


def display_name_for_family(family_id: str) -> str:
    return family_id.replace("-", " ").replace("_", " ").strip().title()


def suggest_repo_names(*, family_id: str, repos: list[dict[str, Any]]) -> list[str]:
    matches = []
    for row in repos:
        full_name = row.get("full_name", "")
        if family_id not in full_name.casefold():
            continue
        matches.append((-(int(row.get("stars", 0) or 0)), full_name))
    matches.sort()
    return [full_name for _, full_name in matches[:3]]


def build_benchmark_overlap_lookup(runtime: RuntimeConfig | None) -> dict[str, int]:
    if runtime is None:
        return {}
    overlap: Counter[str] = Counter()
    for entity in runtime.benchmarks.entities:
        tokens = {entity.entity_id.casefold()}
        for prefix in entity.package_prefixes:
            tokens.add(infer_candidate_family_id(prefix))
        for repo_name in entity.repo_names:
            _, _, repo = repo_name.casefold().partition("/")
            tokens.add(repo)
        for token in tokens:
            overlap[token] += 1
    return dict(overlap)
