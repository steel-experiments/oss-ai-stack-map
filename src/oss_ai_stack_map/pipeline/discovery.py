from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import DiscoveredRepo, DiscoveryResult, StageTiming
from oss_ai_stack_map.storage.tables import write_rows


def discover_candidates(
    runtime: RuntimeConfig,
    client: GitHubClient,
    output_dir: Path,
    progress: Callable[[str], None] | None = None,
) -> DiscoveryResult:
    started_at = time.perf_counter()
    queries = build_queries(runtime)
    raw_items_by_full_name: dict[str, dict] = {}
    query_map: dict[str, set[str]] = {}
    search_started_at = time.perf_counter()

    if progress:
        progress(f"discovery: starting {len(queries)} queries")

    for index, query in enumerate(queries, start=1):
        query_started_at = time.perf_counter()
        query_new_repos = 0
        for page in range(1, runtime.study.filters.max_search_pages_per_query + 1):
            payload = client.search_repositories(query=query, page=page)
            items = payload.get("items", [])
            if not items:
                break
            for item in items:
                max_repos = runtime.study.filters.max_repos
                if max_repos and len(raw_items_by_full_name) >= max_repos:
                    break
                full_name = item["full_name"]
                is_new = full_name not in raw_items_by_full_name
                raw_items_by_full_name.setdefault(full_name, item)
                query_map.setdefault(full_name, set()).add(query)
                if is_new:
                    query_new_repos += 1
            if (
                runtime.study.filters.max_repos
                and len(raw_items_by_full_name) >= runtime.study.filters.max_repos
            ):
                break
        if progress:
            progress(
                "discovery: "
                f"query {index}/{len(queries)} "
                f"added {query_new_repos} repos "
                f"({len(raw_items_by_full_name)} total) "
                f"in {time.perf_counter() - query_started_at:.1f}s"
            )
        if (
            runtime.study.filters.max_repos
            and len(raw_items_by_full_name) >= runtime.study.filters.max_repos
        ):
            break

    search_seconds = time.perf_counter() - search_started_at
    hydrate_started_at = time.perf_counter()
    hydrated = hydrate_discovered_repos(
        runtime=runtime,
        client=client,
        raw_items_by_full_name=raw_items_by_full_name,
        query_map=query_map,
        progress=progress,
    )
    hydrate_seconds = time.perf_counter() - hydrate_started_at
    ordered = sorted(hydrated, key=lambda repo: repo.stars, reverse=True)
    write_rows(
        output_dir,
        "repos",
        [repo.to_row() for repo in ordered],
        write_csv=runtime.study.outputs.write_csv,
    )
    stage_timings = [
        StageTiming(
            stage_id="discovery_search",
            seconds=search_seconds,
            item_count=len(queries),
            notes="github repository search queries",
        ),
        StageTiming(
            stage_id="discovery_hydration",
            seconds=hydrate_seconds,
            item_count=len(ordered),
            notes="graphql metadata hydration",
        ),
        StageTiming(
            stage_id="discovery_total",
            seconds=time.perf_counter() - started_at,
            item_count=len(ordered),
        ),
    ]
    write_rows(
        output_dir,
        "discovery_stage_timings",
        [timing.to_row() for timing in stage_timings],
        write_csv=runtime.study.outputs.write_csv,
    )
    return DiscoveryResult(repositories=ordered, queries=queries, stage_timings=stage_timings)


def build_queries(runtime: RuntimeConfig) -> list[str]:
    base_filters = (
        f"archived:false fork:false template:false "
        f"stars:>={runtime.study.filters.candidate_stars_min} "
        f"pushed:>={_freshness_cutoff(runtime)}"
    )
    queries: list[str] = []
    for topic in runtime.discovery.topics:
        queries.append(f"topic:{topic} {base_filters}")
    for keyword in runtime.discovery.description_keywords:
        queries.append(f'"{keyword}" in:description {base_filters}')
    for repo in runtime.discovery.manual_seed_repos:
        queries.append(f"repo:{repo}")
    return queries


def normalize_repo(
    item: dict,
    discovery_queries: list[str],
    runtime: RuntimeConfig,
    hydrated_repo: dict | None = None,
) -> DiscoveredRepo:
    if hydrated_repo:
        return normalize_graphql_repo(hydrated_repo, discovery_queries, runtime)

    topics = item.get("topics") or []
    license_info = item.get("license") or {}
    return DiscoveredRepo(
        repo_id=item["id"],
        full_name=item["full_name"],
        html_url=item["html_url"],
        description=item.get("description"),
        owner_type=item.get("owner", {}).get("type"),
        stars=item.get("stargazers_count", 0),
        forks=item.get("forks_count", 0),
        primary_language=item.get("language"),
        topics=topics,
        license_spdx=license_info.get("spdx_id"),
        is_archived=item.get("archived", False),
        is_fork=item.get("fork", False),
        is_template=item.get("is_template", False),
        created_at=item["created_at"],
        updated_at=item["updated_at"],
        pushed_at=item["pushed_at"],
        snapshot_date=runtime.study.snapshot_date,
        discovery_queries=discovery_queries,
    )


def hydrate_discovered_repos(
    runtime: RuntimeConfig,
    client: GitHubClient,
    raw_items_by_full_name: dict[str, dict],
    query_map: dict[str, set[str]],
    progress: Callable[[str], None] | None = None,
) -> list[DiscoveredRepo]:
    full_names = list(raw_items_by_full_name)
    hydrated: dict[str, dict] = {}
    batches = chunked(full_names, 20)
    for index, batch in enumerate(batches, start=1):
        hydrated.update(client.get_repositories_metadata(batch))
        if progress and (index == 1 or index == len(batches) or index % 10 == 0):
            progress(
                "discovery: "
                f"hydrated metadata batch {index}/{len(batches)} "
                f"({min(index * 20, len(full_names))}/{len(full_names)} repos)"
            )

    repos: list[DiscoveredRepo] = []
    for full_name, item in raw_items_by_full_name.items():
        repos.append(
            normalize_repo(
                item=item,
                discovery_queries=sorted(query_map.get(full_name, set())),
                runtime=runtime,
                hydrated_repo=hydrated.get(full_name),
            )
        )
    return repos


def normalize_graphql_repo(
    repo: dict,
    discovery_queries: list[str],
    runtime: RuntimeConfig,
) -> DiscoveredRepo:
    topics = [
        node.get("topic", {}).get("name")
        for node in repo.get("repositoryTopics", {}).get("nodes", [])
        if node.get("topic", {}).get("name")
    ]
    owner = repo.get("owner") or {}
    primary_language = repo.get("primaryLanguage") or {}
    license_info = repo.get("licenseInfo") or {}
    return DiscoveredRepo(
        repo_id=repo["databaseId"],
        full_name=repo["nameWithOwner"],
        html_url=repo["url"],
        description=repo.get("description"),
        owner_type=owner.get("__typename"),
        stars=repo.get("stargazerCount", 0),
        forks=repo.get("forkCount", 0),
        primary_language=primary_language.get("name"),
        topics=topics,
        license_spdx=license_info.get("spdxId"),
        is_archived=repo.get("isArchived", False),
        is_fork=repo.get("isFork", False),
        is_template=repo.get("isTemplate", False),
        created_at=repo["createdAt"],
        updated_at=repo["updatedAt"],
        pushed_at=repo["pushedAt"],
        snapshot_date=runtime.study.snapshot_date,
        discovery_queries=discovery_queries,
    )


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _freshness_cutoff(runtime: RuntimeConfig) -> str:
    year = runtime.study.snapshot_date.year
    month = runtime.study.snapshot_date.month - runtime.study.filters.freshness_months
    while month <= 0:
        year -= 1
        month += 12
    day = min(runtime.study.snapshot_date.day, 28)
    return f"{year:04d}-{month:02d}-{day:02d}"
