from __future__ import annotations

import json
import math
import random
import re
import time
import tomllib
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import unquote

import httpx

from oss_ai_stack_map.config.loader import RuntimeConfig, TechnologyAlias
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    ClassificationSummary,
    DiscoveredRepo,
    JudgeDecision,
    ManifestDependency,
    RepoContext,
    RepoContextCacheEntry,
    StageTiming,
)
from oss_ai_stack_map.openai.judge import OpenAIJudge
from oss_ai_stack_map.pipeline.imports import (
    collect_import_dependencies,
    dedupe_import_dependencies,
)
from oss_ai_stack_map.pipeline.imports import (
    resolve_alias as resolve_import_alias,
)
from oss_ai_stack_map.pipeline.normalize import (
    build_readme_alias_evidence,
    build_repo_technology_edges,
    build_technology_rows,
)
from oss_ai_stack_map.storage.checkpoints import ClassificationCheckpointStore, stable_hash
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows, write_rows_to_paths


@dataclass
class JudgeCandidate:
    decision: ClassificationDecision
    judge_mode: str


@dataclass(frozen=True)
class TechnologyMatch:
    technology_id: str
    provider_id: str | None = None
    provider_technology_id: str | None = None
    entity_type: str | None = None
    canonical_product_id: str | None = None
    match_method: str | None = None


@dataclass
class ProcessedRepo:
    repo: DiscoveredRepo
    context: RepoContext
    decision: ClassificationDecision
    context_cache_entry: RepoContextCacheEntry | None
    cache_hit: bool


def classify_candidates(
    runtime: RuntimeConfig,
    client: GitHubClient,
    input_dir: Path,
    output_dir: Path,
    limit: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> ClassificationSummary:
    started_at = time.perf_counter()
    repos = read_parquet_models(input_dir / "repos.parquet", DiscoveredRepo)
    if limit is not None:
        repos = repos[:limit]
    all_repos = repos
    total_repos = len(repos)
    checkpoint_store = ClassificationCheckpointStore(
        output_dir=output_dir,
        write_csv=runtime.study.outputs.write_csv,
    )
    checkpoint_store.ensure_compatible_run(
        runtime=runtime,
        repo_ids=[repo.repo_id for repo in repos],
    )
    completed_repo_ids = checkpoint_store.load_completed_repo_ids()
    repos = [repo for repo in repos if repo.repo_id not in completed_repo_ids]
    processed_new_repos = bool(repos)

    alias_lookup = runtime.aliases.alias_lookup()
    context_cache_config = context_cache_config_hash(runtime)
    context_cache_by_key = load_context_cache(runtime)
    context_started_at = time.perf_counter()

    if progress:
        if completed_repo_ids:
            progress(
                "classification: "
                f"resuming from checkpoints, skipping {len(completed_repo_ids)} completed repos"
            )
        progress(f"classification: building context for {len(repos)} repos")

    batch_contexts: list[RepoContext] = []
    batch_decisions: list[ClassificationDecision] = []
    batch_context_cache_entries: list[RepoContextCacheEntry] = []
    processed_repo_count = len(completed_repo_ids)
    batch_index = checkpoint_store.next_batch_index()
    cache_hit_count = 0
    cache_write_count = 0

    worker_count = classification_worker_count(runtime)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for batch_start in range(0, len(repos), runtime.study.checkpoint_batch_size):
            repo_batch = repos[batch_start : batch_start + runtime.study.checkpoint_batch_size]
            processed_batch = process_repo_batch(
                runtime=runtime,
                client=client,
                repos=repo_batch,
                alias_lookup=alias_lookup,
                context_cache_by_key=context_cache_by_key,
                context_config_hash=context_cache_config,
                executor=executor,
            )
            for processed in processed_batch:
                batch_contexts.append(processed.context)
                batch_decisions.append(processed.decision)
                if processed.context_cache_entry is not None:
                    batch_context_cache_entries.append(processed.context_cache_entry)
                if processed.cache_hit:
                    cache_hit_count += 1
            if not batch_contexts:
                continue
            flush_classification_checkpoint_batch(
                checkpoint_store=checkpoint_store,
                runtime=runtime,
                contexts=batch_contexts,
                decisions=batch_decisions,
                batch_index=batch_index,
            )
            if batch_context_cache_entries:
                persist_context_cache_entries(
                    runtime=runtime,
                    entries=batch_context_cache_entries,
                )
                for entry in batch_context_cache_entries:
                    context_cache_by_key[
                        context_cache_key(
                            repo_full_name=entry.repo_full_name,
                            repo_pushed_at=entry.repo_pushed_at,
                            context_config_hash=entry.context_config_hash,
                        )
                    ] = entry
                cache_write_count += len(batch_context_cache_entries)
            processed_repo_count += len(batch_contexts)
            checkpoint_store.update_progress(
                processed_repo_count=processed_repo_count,
                total_repos=total_repos,
                stage="classification_context_build",
            )
            if progress:
                progress(
                    "classification: "
                    f"checkpointed batch {batch_index} "
                    f"({processed_repo_count}/{total_repos} repos)"
                )
            batch_index += 1
            batch_contexts = []
            batch_decisions = []
            batch_context_cache_entries = []
            if progress:
                for offset, processed in enumerate(processed_batch, start=1):
                    index = batch_start + offset
                    if index <= 3 or index == len(repos) or index % 50 == 0:
                        progress(
                            "classification: "
                            f"processed {index}/{total_repos} repos "
                            f"(latest: {processed.repo.full_name})"
                        )

    context_seconds = time.perf_counter() - context_started_at
    checkpoint_store.update_progress(
        processed_repo_count=processed_repo_count,
        total_repos=total_repos,
        stage="classification_finalize",
    )

    decisions = checkpoint_store.read_checkpoint_models(
        "repo_inclusion_decisions", ClassificationDecision
    )
    existing_judge_decisions = load_existing_judge_decisions(output_dir=output_dir)
    if existing_judge_decisions:
        apply_judge_decisions(
            runtime=runtime,
            decisions=decisions,
            judge_decisions=existing_judge_decisions,
        )

    contexts = checkpoint_store.read_checkpoint_models("repo_contexts", RepoContext)
    contexts_by_id = {context.repo_id: context for context in contexts}

    judge_candidates = select_judge_candidates(
        runtime=runtime,
        decisions=decisions,
        contexts_by_id=contexts_by_id,
        already_judged_repo_ids={decision.repo_id for decision in existing_judge_decisions},
    )
    judge_candidate_repo_ids = {candidate.decision.repo_id for candidate in judge_candidates}
    judge_started_at = time.perf_counter()
    judge_decisions = maybe_run_judge(
        runtime=runtime,
        repos_by_id={repo.repo_id: repo for repo in all_repos},
        contexts_by_id={
            repo_id: context
            for repo_id, context in contexts_by_id.items()
            if repo_id in judge_candidate_repo_ids
        },
        candidates=judge_candidates,
    )
    judge_seconds = time.perf_counter() - judge_started_at
    if judge_decisions:
        apply_judge_decisions(runtime=runtime, decisions=decisions, judge_decisions=judge_decisions)
    all_judge_decisions = merge_judge_decisions(existing_judge_decisions, judge_decisions)

    write_started_at = time.perf_counter()
    should_write_contexts = processed_new_repos or not (
        output_dir / "repo_contexts.parquet"
    ).exists()
    should_write_dependency_evidence = processed_new_repos or not (
        output_dir / "repo_dependency_evidence.parquet"
    ).exists()
    if should_write_contexts:
        write_rows(
            output_dir,
            "repo_contexts",
            [context.to_row() for context in contexts],
            write_csv=runtime.study.outputs.write_csv,
        )
    write_rows(
        output_dir,
        "repo_inclusion_decisions",
        [decision.to_row() for decision in decisions],
        write_csv=runtime.study.outputs.write_csv,
    )
    edge_rows = checkpoint_store.read_checkpoint_rows("repo_dependency_evidence")
    if edge_rows and should_write_dependency_evidence:
        write_rows(
            output_dir,
            "repo_dependency_evidence",
            edge_rows,
            write_csv=runtime.study.outputs.write_csv,
        )
    technology_edges = build_repo_technology_edges(
        runtime=runtime,
        contexts=contexts,
        decisions=decisions,
    )
    if technology_edges:
        write_rows(
            output_dir,
            "repo_technology_edges",
            [edge.to_row() for edge in technology_edges],
            write_csv=runtime.study.outputs.write_csv,
        )
    if all_judge_decisions:
        write_rows(
            output_dir,
            "judge_decisions",
            [decision.to_row() for decision in all_judge_decisions],
            write_csv=runtime.study.outputs.write_csv,
        )
    write_rows(
        output_dir,
        "technologies",
        build_technology_rows(runtime),
        write_csv=runtime.study.outputs.write_csv,
    )
    write_seconds = time.perf_counter() - write_started_at

    passed_serious = sum(1 for decision in decisions if decision.passed_serious_filter)
    passed_ai = sum(1 for decision in decisions if decision.passed_ai_relevance_filter)
    passed_major = sum(1 for decision in decisions if decision.passed_major_filter)
    stage_timings = [
        StageTiming(
            stage_id="classification_context_build",
            seconds=context_seconds,
            item_count=total_repos,
            notes=(
                "repo readme/tree/manifest/sbom/import processing "
                f"(context cache hits={cache_hit_count}, writes={cache_write_count})"
            ),
        ),
        StageTiming(
            stage_id="classification_judge",
            seconds=judge_seconds,
            item_count=len(judge_decisions),
            notes="optional llm judge pass",
        ),
        StageTiming(
            stage_id="classification_write_outputs",
            seconds=write_seconds,
            item_count=len(decisions),
        ),
        StageTiming(
            stage_id="classification_total",
            seconds=time.perf_counter() - started_at,
            item_count=total_repos,
        ),
    ]
    checkpoint_store.write_stage_timings_checkpoint(stage_timings)
    write_rows(
        output_dir,
        "classification_stage_timings",
        [timing.to_row() for timing in stage_timings],
        write_csv=runtime.study.outputs.write_csv,
    )
    checkpoint_store.mark_completed(
        processed_repo_count=len(decisions),
        total_repos=total_repos,
    )
    return ClassificationSummary(
        total=len(decisions),
        passed_serious=passed_serious,
        passed_ai=passed_ai,
        passed_major=passed_major,
        stage_timings=stage_timings,
    )


def flush_classification_checkpoint_batch(
    checkpoint_store: ClassificationCheckpointStore,
    runtime: RuntimeConfig,
    contexts: list[RepoContext],
    decisions: list[ClassificationDecision],
    batch_index: int,
) -> None:
    checkpoint_store.write_batch_rows(
        "repo_contexts",
        batch_index,
        [context.to_row() for context in contexts],
    )
    dependency_rows = [
        dep.to_row(repo_id=context.repo_id, snapshot_date=runtime.study.snapshot_date)
        for context in contexts
        for dep in (
            context.manifest_dependencies
            + context.sbom_dependencies
            + context.import_dependencies
        )
    ]
    checkpoint_store.write_batch_rows(
        "repo_dependency_evidence",
        batch_index,
        dependency_rows,
    )
    checkpoint_store.write_batch_rows(
        "repo_inclusion_decisions",
        batch_index,
        [decision.to_row() for decision in decisions],
    )


def classification_worker_count(runtime: RuntimeConfig) -> int:
    return max(1, min(8, runtime.study.checkpoint_batch_size))


def process_repo_batch(
    *,
    runtime: RuntimeConfig,
    client: GitHubClient,
    repos: list[DiscoveredRepo],
    alias_lookup: dict[str, TechnologyAlias],
    context_cache_by_key: dict[tuple[str, str, str], RepoContextCacheEntry],
    context_config_hash: str,
    executor: ThreadPoolExecutor,
) -> list[ProcessedRepo]:
    ordered_results: dict[int, ProcessedRepo] = {}
    uncached_repos: list[DiscoveredRepo] = []

    for repo in repos:
        cached_context = load_cached_repo_context(
            cache_by_key=context_cache_by_key,
            repo=repo,
            context_config_hash=context_config_hash,
        )
        if cached_context is None:
            uncached_repos.append(repo)
            continue
        ordered_results[repo.repo_id] = ProcessedRepo(
            repo=repo,
            context=cached_context,
            decision=classify_repo(
                runtime=runtime,
                repo=repo,
                context=cached_context,
                alias_lookup=alias_lookup,
            ),
            context_cache_entry=None,
            cache_hit=True,
        )

    if not uncached_repos:
        return [ordered_results[repo.repo_id] for repo in repos]

    if len(uncached_repos) == 1 or classification_worker_count(runtime) == 1:
        for repo in uncached_repos:
            processed = process_uncached_repo(
                runtime=runtime,
                client=client,
                repo=repo,
                alias_lookup=alias_lookup,
                context_config_hash=context_config_hash,
            )
            ordered_results[repo.repo_id] = processed
        return [ordered_results[repo.repo_id] for repo in repos]

    future_by_repo_id: dict[int, Future[ProcessedRepo]] = {
        repo.repo_id: executor.submit(
            process_uncached_repo,
            runtime=runtime,
            client=client,
            repo=repo,
            alias_lookup=alias_lookup,
            context_config_hash=context_config_hash,
        )
        for repo in uncached_repos
    }
    for future in as_completed(future_by_repo_id.values()):
        processed = future.result()
        ordered_results[processed.repo.repo_id] = processed
    return [ordered_results[repo.repo_id] for repo in repos]


def process_uncached_repo(
    *,
    runtime: RuntimeConfig,
    client: GitHubClient,
    repo: DiscoveredRepo,
    alias_lookup: dict[str, TechnologyAlias],
    context_config_hash: str,
) -> ProcessedRepo:
    context = build_repo_context(
        runtime=runtime,
        client=client,
        repo=repo,
        alias_lookup=alias_lookup,
    )
    return ProcessedRepo(
        repo=repo,
        context=context,
        decision=classify_repo(
            runtime=runtime,
            repo=repo,
            context=context,
            alias_lookup=alias_lookup,
        ),
        context_cache_entry=RepoContextCacheEntry(
            repo_full_name=repo.full_name,
            repo_pushed_at=repo.pushed_at,
            context_config_hash=context_config_hash,
            context=context,
        ),
        cache_hit=False,
    )


def context_cache_path(runtime: RuntimeConfig) -> Path:
    return (
        Path("data/raw/classification_context_cache")
        / runtime.study.snapshot_date.isoformat()
        / "repo_contexts.parquet"
    )


def context_cache_config_hash(runtime: RuntimeConfig) -> str:
    payload = {
        "study": {
            "classification": runtime.study.classification.model_dump(mode="json"),
        },
        "discovery": runtime.discovery.model_dump(mode="json"),
        "exclusions": runtime.exclusions.model_dump(mode="json"),
        "aliases": runtime.aliases.model_dump(mode="json"),
        "registry": runtime.registry.model_dump(mode="json"),
    }
    return stable_hash(payload)


def context_cache_key(
    *,
    repo_full_name: str,
    repo_pushed_at: str,
    context_config_hash: str,
) -> tuple[str, str, str]:
    return (repo_full_name.casefold(), repo_pushed_at, context_config_hash)


def load_context_cache(runtime: RuntimeConfig) -> dict[tuple[str, str, str], RepoContextCacheEntry]:
    path = context_cache_path(runtime)
    if not path.exists():
        return {}
    entries = read_parquet_models(path, RepoContextCacheEntry)
    return {
        context_cache_key(
            repo_full_name=entry.repo_full_name,
            repo_pushed_at=entry.repo_pushed_at,
            context_config_hash=entry.context_config_hash,
        ): entry
        for entry in entries
    }


def load_cached_repo_context(
    *,
    cache_by_key: dict[tuple[str, str, str], RepoContextCacheEntry],
    repo: DiscoveredRepo,
    context_config_hash: str,
) -> RepoContext | None:
    entry = cache_by_key.get(
        context_cache_key(
            repo_full_name=repo.full_name,
            repo_pushed_at=repo.pushed_at,
            context_config_hash=context_config_hash,
        )
    )
    if entry is None:
        return None
    return entry.context.model_copy(
        update={
            "repo_id": repo.repo_id,
            "full_name": repo.full_name,
        }
    )


def persist_context_cache_entries(
    runtime: RuntimeConfig,
    entries: list[RepoContextCacheEntry],
) -> None:
    if not entries:
        return
    path = context_cache_path(runtime)
    existing = read_parquet_models(path, RepoContextCacheEntry) if path.exists() else []
    merged = {
        context_cache_key(
            repo_full_name=entry.repo_full_name,
            repo_pushed_at=entry.repo_pushed_at,
            context_config_hash=entry.context_config_hash,
        ): entry
        for entry in existing
    }
    for entry in entries:
        merged[
            context_cache_key(
                repo_full_name=entry.repo_full_name,
                repo_pushed_at=entry.repo_pushed_at,
                context_config_hash=entry.context_config_hash,
            )
        ] = entry
    write_rows_to_paths(
        rows=[
            merged[key].to_row()
            for key in sorted(merged)
        ],
        parquet_path=path,
        csv_path=None,
    )


def select_judge_candidates(
    runtime: RuntimeConfig,
    decisions: list[ClassificationDecision],
    contexts_by_id: dict[int, RepoContext] | None = None,
    already_judged_repo_ids: set[int] | None = None,
) -> list[JudgeCandidate]:
    already_judged_repo_ids = already_judged_repo_ids or set()
    max_cases = runtime.study.judge.max_cases_per_run
    hardening_candidates: list[JudgeCandidate] = []
    if runtime.study.judge.mode_enabled("hardening"):
        hardening_candidates = [
            JudgeCandidate(decision=decision, judge_mode="hardening")
            for decision in decisions
            if decision.repo_id not in already_judged_repo_ids
            and should_send_to_judge(runtime=runtime, decision=decision)
        ][:max_cases]
    if len(hardening_candidates) >= max_cases:
        return hardening_candidates

    selected_repo_ids = {
        candidate.decision.repo_id for candidate in hardening_candidates
    } | already_judged_repo_ids
    validation_candidates: list[JudgeCandidate] = []
    if runtime.study.judge.mode_enabled("validation"):
        validation_pool = [
            decision
            for decision in decisions
            if decision.repo_id not in selected_repo_ids
            and should_send_to_validation_judge(decision)
        ]
        target_case_count = runtime.study.judge.validation_target_case_count(
            final_repo_count=len(validation_pool),
            remaining_capacity=max_cases - len(hardening_candidates),
        )
        sampled_validation_decisions = sample_validation_decisions(
            runtime=runtime,
            decisions=validation_pool,
            contexts_by_id=contexts_by_id or {},
            target_case_count=target_case_count,
        )
        validation_candidates = [
            JudgeCandidate(decision=decision, judge_mode="validation")
            for decision in sampled_validation_decisions
        ]
    return [*hardening_candidates, *validation_candidates]


def should_send_to_validation_judge(decision: ClassificationDecision) -> bool:
    return decision.passed_major_filter


def sample_validation_decisions(
    *,
    runtime: RuntimeConfig,
    decisions: list[ClassificationDecision],
    contexts_by_id: dict[int, RepoContext],
    target_case_count: int,
) -> list[ClassificationDecision]:
    if target_case_count <= 0 or not decisions:
        return []
    ordered = sorted(decisions, key=lambda decision: decision.repo_id)
    if target_case_count >= len(ordered):
        return ordered

    fraction = runtime.study.judge.validation_sample_fraction
    if fraction is None:
        return ordered[:target_case_count]

    rng = random.Random(runtime.study.judge.validation_sample_seed)
    selected: list[ClassificationDecision] = []
    selected_repo_ids: set[int] = set()
    profiles = {
        decision.repo_id: build_validation_sampling_profile(
            runtime=runtime,
            decision=decision,
            context=contexts_by_id.get(decision.repo_id),
        )
        for decision in ordered
    }

    def take_from_pool(pool: list[ClassificationDecision], count: int) -> None:
        if count <= 0:
            return
        shuffled = [decision for decision in pool if decision.repo_id not in selected_repo_ids]
        rng.shuffle(shuffled)
        for decision in shuffled[:count]:
            selected.append(decision)
            selected_repo_ids.add(decision.repo_id)

    readme_pool = [
        decision for decision in ordered if profiles[decision.repo_id]["evidence_profile"] == "readme_only"
    ]
    override_pool = [
        decision for decision in ordered if profiles[decision.repo_id]["prior_override"]
    ]
    readme_floor = min(len(readme_pool), max(1, math.ceil(target_case_count * 0.15))) if readme_pool else 0
    override_floor = min(len(override_pool), max(1, math.ceil(target_case_count * 0.20))) if override_pool else 0
    take_from_pool(readme_pool, readme_floor)
    take_from_pool(override_pool, override_floor)

    segment_buckets: dict[str, list[ClassificationDecision]] = {}
    for decision in ordered:
        segment_key = profiles[decision.repo_id]["segment"]
        segment_buckets.setdefault(segment_key, []).append(decision)
    for bucket in segment_buckets.values():
        rng.shuffle(bucket)
    for segment_key in sorted(segment_buckets, key=lambda key: (-len(segment_buckets[key]), key)):
        if len(selected) >= target_case_count:
            break
        for decision in segment_buckets[segment_key]:
            if decision.repo_id in selected_repo_ids:
                continue
            selected.append(decision)
            selected_repo_ids.add(decision.repo_id)
            break

    if len(selected) >= target_case_count:
        return sorted(selected[:target_case_count], key=lambda decision: decision.repo_id)

    strata: dict[tuple[str, str, str, str, str], list[ClassificationDecision]] = {}
    for decision in ordered:
        profile = profiles[decision.repo_id]
        key = (
            profile["segment"],
            profile["score_ai_band"],
            profile["score_serious_band"],
            profile["evidence_profile"],
            "prior_override" if profile["prior_override"] else "rule_only",
        )
        strata.setdefault(key, []).append(decision)
    for bucket in strata.values():
        rng.shuffle(bucket)

    prioritized_keys = sorted(
        strata,
        key=lambda key: (
            key[3] != "readme_only",
            key[4] != "prior_override",
            -len(strata[key]),
            key,
        ),
    )
    while len(selected) < target_case_count:
        progressed = False
        for key in prioritized_keys:
            if len(selected) >= target_case_count:
                break
            bucket = strata[key]
            while bucket and bucket[-1].repo_id in selected_repo_ids:
                bucket.pop()
            if not bucket:
                continue
            decision = bucket.pop()
            selected.append(decision)
            selected_repo_ids.add(decision.repo_id)
            progressed = True
        if not progressed:
            break

    return sorted(selected, key=lambda decision: decision.repo_id)


def build_validation_sampling_profile(
    *,
    runtime: RuntimeConfig,
    decision: ClassificationDecision,
    context: RepoContext | None,
) -> dict[str, str | bool]:
    evidence_profile = "unmapped"
    if context is not None:
        evidence_profile = classify_repo_context_evidence_profile(runtime=runtime, context=context)
    return {
        "segment": decision.primary_segment or "unassigned",
        "score_ai_band": score_band(decision.score_ai),
        "score_serious_band": score_band(decision.score_serious),
        "evidence_profile": evidence_profile,
        "prior_override": bool(decision.judge_override_applied),
    }


def classify_repo_context_evidence_profile(
    *,
    runtime: RuntimeConfig,
    context: RepoContext,
) -> str:
    has_direct_evidence = any(
        dependency.technology_id
        for dependency in (
            context.manifest_dependencies + context.sbom_dependencies + context.import_dependencies
        )
    )
    repo_lookup = {
        repo_name.casefold()
        for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]
        for repo_name in technology.repo_names
    }
    has_repo_identity = context.full_name.casefold() in repo_lookup
    has_readme_fallback = bool(
        runtime.study.classification.readme_mentions_used_for_edges
        and build_readme_alias_evidence(runtime=runtime, context=context)
    )
    if has_direct_evidence:
        return "direct_only" if not has_readme_fallback else "mixed_direct_and_fallback"
    if has_repo_identity and has_readme_fallback:
        return "repo_identity_plus_fallback"
    if has_repo_identity:
        return "repo_identity_only"
    if has_readme_fallback:
        return "readme_only"
    return "unmapped"


def score_band(value: int) -> str:
    if value <= 1:
        return "0-1"
    if value <= 3:
        return "2-3"
    if value <= 5:
        return "4-5"
    if value <= 8:
        return "6-8"
    return "9+"


def maybe_run_judge(
    runtime: RuntimeConfig,
    repos_by_id: dict[int, DiscoveredRepo],
    contexts_by_id: dict[int, RepoContext],
    candidates: list[JudgeCandidate],
) -> list[JudgeDecision]:
    if (
        not runtime.study.judge.any_mode_enabled()
        or not runtime.env.openai_api_key
        or not candidates
    ):
        return []

    outputs: list[JudgeDecision] = []
    with OpenAIJudge(runtime=runtime) as judge:
        for candidate in candidates:
            decision = candidate.decision
            repo = repos_by_id.get(decision.repo_id)
            context = contexts_by_id.get(decision.repo_id)
            if repo is None or context is None:
                decision.notes.append(f"{candidate.judge_mode} judge skipped: missing repo context")
                continue
            try:
                outputs.append(
                    judge.judge_repo(
                        repo=repo,
                        context=context,
                        decision=decision,
                        judge_mode=candidate.judge_mode,
                    )
                )
            except Exception as exc:
                decision.notes.append(
                    f"{candidate.judge_mode} judge failed: {exc.__class__.__name__}"
                )
    return outputs


def load_existing_judge_decisions(output_dir: Path) -> list[JudgeDecision]:
    path = output_dir / "judge_decisions.parquet"
    if not path.exists():
        return []
    return read_parquet_models(path, JudgeDecision)


def merge_judge_decisions(
    existing: list[JudgeDecision],
    new: list[JudgeDecision],
) -> list[JudgeDecision]:
    merged = {decision.repo_id: decision for decision in existing}
    for decision in new:
        merged[decision.repo_id] = decision
    return [merged[repo_id] for repo_id in sorted(merged)]


def should_send_to_judge(
    runtime: RuntimeConfig,
    decision: ClassificationDecision,
) -> bool:
    if decision.exclusion_reason is not None:
        return True
    if decision.score_serious in {
        runtime.study.classification.serious_pass_score - 1,
        runtime.study.classification.serious_pass_score,
        runtime.study.classification.serious_pass_score + 1,
    }:
        return True
    if decision.score_ai in {
        runtime.study.classification.ai_relevance_pass_score - 1,
        runtime.study.classification.ai_relevance_pass_score,
        runtime.study.classification.ai_relevance_pass_score + 1,
    }:
        return True
    interesting_notes = {
        "educational material signal",
        "mostly docs or notebooks",
        "code only in examples",
        "AI relevance inferred from direct repo config",
        "single exclusion keyword overridden by strong structural signals",
    }
    return any(note in interesting_notes for note in decision.notes)


def apply_judge_decisions(
    runtime: RuntimeConfig,
    decisions: list[ClassificationDecision],
    judge_decisions: list[JudgeDecision],
) -> None:
    judge_by_repo = {decision.repo_id: decision for decision in judge_decisions}
    confidence_order = {"low": 1, "medium": 2, "high": 3}
    min_confidence = confidence_order.get(runtime.study.judge.min_confidence_to_override, 3)

    for decision in decisions:
        judge = judge_by_repo.get(decision.repo_id)
        if judge is None:
            continue

        normalized_judge_primary_segment = normalize_judge_primary_segment(
            runtime=runtime,
            primary_segment=judge.primary_segment,
        )
        if judge.primary_segment and normalized_judge_primary_segment is None:
            decision.notes.append(
                f"{judge.judge_mode} judge segment ignored: {judge.primary_segment}"
            )

        # Preserve the original rule-engine outputs across repeated judge passes.
        # Validation may be run on top of a prior hardening snapshot, and the
        # raw rule_* fields should remain the pre-judge baseline for auditability.
        if decision.rule_passed_serious_filter is None:
            decision.rule_passed_serious_filter = decision.passed_serious_filter
        if decision.rule_passed_ai_relevance_filter is None:
            decision.rule_passed_ai_relevance_filter = decision.passed_ai_relevance_filter
        if decision.rule_passed_major_filter is None:
            decision.rule_passed_major_filter = decision.passed_major_filter
        decision.judge_applied = True
        decision.judge_mode = judge.judge_mode
        decision.judge_confidence = judge.confidence
        decision.judge_include_in_final_set = judge.include_in_final_set
        decision.judge_serious_project = judge.serious_project
        decision.judge_ai_relevant = judge.ai_relevant
        decision.judge_primary_segment = normalized_judge_primary_segment
        decision.judge_reasons = judge.reasons

        judge_changes_rule = (
            decision.passed_serious_filter != judge.serious_project
            or decision.passed_ai_relevance_filter != judge.ai_relevant
            or decision.passed_major_filter != judge.include_in_final_set
            or (
                normalized_judge_primary_segment is not None
                and decision.primary_segment != normalized_judge_primary_segment
            )
        )
        hardening_is_authoritative = (
            judge.judge_mode == "hardening"
            and runtime.study.judge.override_on_high_confidence
            and confidence_order.get(judge.confidence, 0) >= min_confidence
        )
        validation_is_authoritative = judge.judge_mode == "validation"
        judge_is_authoritative = validation_is_authoritative or hardening_is_authoritative
        judge.applied = judge_is_authoritative and judge_changes_rule

        if not judge_is_authoritative:
            decision.notes.append(f"{judge.judge_mode} judge reviewed without override")
            continue

        decision.passed_serious_filter = judge.serious_project
        decision.passed_ai_relevance_filter = judge.ai_relevant
        decision.passed_major_filter = judge.include_in_final_set
        if normalized_judge_primary_segment:
            decision.primary_segment = normalized_judge_primary_segment
        if judge_changes_rule:
            decision.judge_override_applied = True
            decision.notes.append(f"{judge.judge_mode} judge override applied")
        else:
            decision.notes.append(f"{judge.judge_mode} judge reviewed without override")


def configured_segment_ids(runtime: RuntimeConfig) -> set[str]:
    return {rule.segment_id for rule in runtime.segments.rules} | set(runtime.segments.precedence)


def normalize_judge_primary_segment(
    *,
    runtime: RuntimeConfig,
    primary_segment: str | None,
) -> str | None:
    if primary_segment is None:
        return None
    normalized = primary_segment.strip()
    if not normalized:
        return None

    allowed = configured_segment_ids(runtime)
    if normalized in allowed:
        return normalized

    normalized_key = (
        normalized.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
    )
    by_key = {
        segment_id.casefold().replace("-", "_").replace(" ", "_").replace("/", "_"): segment_id
        for segment_id in allowed
    }
    return by_key.get(normalized_key)


def build_repo_context(
    runtime: RuntimeConfig,
    client: GitHubClient,
    repo: DiscoveredRepo,
    alias_lookup: dict[str, TechnologyAlias],
) -> RepoContext:
    owner, name = repo.full_name.split("/", 1)
    readme_text = safe_call(lambda: client.get_readme(owner, name), default="")
    tree_paths = safe_call(
        lambda: client.get_tree(owner, name, repo.default_branch),
        default=[],
    )
    manifest_paths = find_manifest_paths(tree_paths, runtime)
    registry_lookup = runtime.registry.alias_lookup()
    registry_prefix_rules = runtime.registry.package_prefix_rules()
    import_lookup = runtime.registry.import_lookup()
    import_lookup.update(runtime.aliases.import_lookup())

    manifest_dependencies: list[ManifestDependency] = []
    for path in manifest_paths:
        text = safe_call(lambda path=path: client.get_file_text(owner, name, path), default="")
        manifest_dependencies.extend(
            parse_manifest_dependencies(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        )
    sbom_payload = safe_call(
        lambda: client.get_sbom(owner, name),
        default={},
        reraise_status_codes={403, 429},
    )
    sbom_dependencies = parse_sbom_dependencies(
        sbom_payload,
        alias_lookup,
        registry_lookup,
        registry_prefix_rules,
    )
    structured_hits = any(dep.technology_id for dep in manifest_dependencies + sbom_dependencies)
    import_dependencies: list[ManifestDependency] = []
    if should_run_import_scan(runtime, repo, manifest_paths, structured_hits):
        import_dependencies = collect_import_dependencies(
            runtime=runtime,
            client=client,
            full_name=repo.full_name,
            tree_paths=tree_paths,
            import_lookup=import_lookup,
        )

    return RepoContext(
        repo_id=repo.repo_id,
        full_name=repo.full_name,
        default_branch=repo.default_branch,
        readme_text=readme_text,
        tree_paths=tree_paths,
        manifest_paths=manifest_paths,
        manifest_dependencies=dedupe_dependencies(manifest_dependencies),
        sbom_dependencies=dedupe_dependencies(sbom_dependencies),
        import_dependencies=dedupe_dependencies(import_dependencies),
    )


def rebind_repo_context(
    runtime: RuntimeConfig,
    context: RepoContext,
) -> RepoContext:
    alias_lookup = runtime.aliases.alias_lookup()
    registry_lookup = runtime.registry.alias_lookup()
    registry_prefix_rules = runtime.registry.package_prefix_rules()
    import_lookup = runtime.registry.import_lookup()
    import_lookup.update(runtime.aliases.import_lookup())
    return RepoContext(
        repo_id=context.repo_id,
        full_name=context.full_name,
        default_branch=context.default_branch,
        readme_text=context.readme_text,
        tree_paths=context.tree_paths,
        manifest_paths=context.manifest_paths,
        manifest_dependencies=dedupe_dependencies(
            [
                rebind_package_dependency(
                    dependency,
                    alias_lookup=alias_lookup,
                    registry_lookup=registry_lookup,
                    registry_prefix_rules=registry_prefix_rules,
                )
                for dependency in context.manifest_dependencies
            ]
        ),
        sbom_dependencies=dedupe_dependencies(
            [
                rebind_package_dependency(
                    dependency,
                    alias_lookup=alias_lookup,
                    registry_lookup=registry_lookup,
                    registry_prefix_rules=registry_prefix_rules,
                )
                for dependency in context.sbom_dependencies
            ]
        ),
        import_dependencies=dedupe_import_dependencies(
            [
                rebind_import_dependency(
                    dependency,
                    import_lookup=import_lookup,
                )
                for dependency in context.import_dependencies
            ]
        ),
    )


def classify_repo(
    runtime: RuntimeConfig,
    repo: DiscoveredRepo,
    context: RepoContext,
    alias_lookup: dict[str, TechnologyAlias],
) -> ClassificationDecision:
    serious_score, serious_notes, exclusion_reason = score_serious(runtime, repo, context)
    passed_serious = (
        exclusion_reason is None
        and serious_score >= runtime.study.classification.serious_pass_score
    )

    ai_score, ai_notes = score_ai_relevance(runtime, repo, context)
    passed_ai = ai_score >= runtime.study.classification.ai_relevance_pass_score and passed_serious

    segments = score_segments(runtime, repo, context)
    primary_segment = segments[0] if segments else None

    return ClassificationDecision(
        repo_id=repo.repo_id,
        full_name=repo.full_name,
        passed_candidate_filter=repo.stars >= runtime.study.filters.candidate_stars_min,
        passed_serious_filter=passed_serious,
        passed_ai_relevance_filter=passed_ai,
        passed_major_filter=passed_serious
        and passed_ai
        and repo.stars >= runtime.study.filters.major_stars_min,
        rule_passed_serious_filter=passed_serious,
        rule_passed_ai_relevance_filter=passed_ai,
        rule_passed_major_filter=passed_serious
        and passed_ai
        and repo.stars >= runtime.study.filters.major_stars_min,
        score_serious=serious_score,
        score_ai=ai_score,
        exclusion_reason=exclusion_reason,
        primary_segment=primary_segment,
        segments=segments,
        notes=serious_notes + ai_notes,
    )


def score_serious(
    runtime: RuntimeConfig, repo: DiscoveredRepo, context: RepoContext
) -> tuple[int, list[str], str | None]:
    score = 0
    notes: list[str] = []

    if context.manifest_paths:
        score += 2
        notes.append("manifest present")
    if any(
        path.endswith(("package-lock.json", "pnpm-lock.yaml", "poetry.lock", "Cargo.lock"))
        for path in context.tree_paths
    ):
        score += 1
        notes.append("lockfile present")
    if has_code_footprint(context.tree_paths, runtime):
        score += 2
        notes.append("code footprint present")
    if any("/test" in path or path.startswith("tests/") for path in context.tree_paths):
        score += 1
        notes.append("tests present")
    if any(path.startswith(".github/workflows/") for path in context.tree_paths):
        score += 1
        notes.append("ci present")
    if any(
        path.startswith(".github/workflows/") and "release" in path.casefold()
        for path in context.tree_paths
    ):
        score += 1
        notes.append("release workflow present")
    if monorepo_subproject_count(context.tree_paths) >= 2:
        score += 1
        notes.append("multiple subprojects")
    if readme_looks_like_product_docs(context.readme_text):
        score += 1
        notes.append("usage-oriented docs")

    if repo.primary_language is None and not context.manifest_paths:
        score -= 2
        notes.append("no manifests and no primary language")
    if mostly_notebooks_or_docs(context.tree_paths):
        score -= 3
        notes.append("mostly docs or notebooks")
    if code_only_in_examples(context.tree_paths, runtime):
        score -= 3
        notes.append("code only in examples")
    if only_prompt_material(context.tree_paths):
        score -= 2
        notes.append("prompt-heavy with minimal code")
    if educational_material_signal(runtime, repo, context.readme_text):
        score -= 4
        notes.append("educational material signal")

    exclusion_term_sources = matched_exclusion_term_sources(runtime, repo)
    exclusion_terms = set(exclusion_term_sources)
    if exclusion_terms:
        single_term = next(iter(exclusion_terms)) if len(exclusion_terms) == 1 else None
        if (
            len(exclusion_terms) > 1
            or score < runtime.study.classification.strong_serious_override_score
            or not can_override_single_exclusion_term(
                term=single_term or "",
                sources=exclusion_term_sources.get(single_term or "", set()),
                context=context,
            )
        ):
            return score, notes, f"hard exclusion: {', '.join(sorted(exclusion_terms))}"
        notes.append("single exclusion keyword overridden by strong structural signals")

    if not has_code_footprint(context.tree_paths, runtime):
        notes.append("missing code footprint signal")

    return score, notes, None


def score_ai_relevance(
    runtime: RuntimeConfig,
    repo: DiscoveredRepo,
    context: RepoContext,
) -> tuple[int, list[str]]:
    score = 0
    notes: list[str] = []

    dependency_signals = 0
    manifest_tech_ids: set[str] = set()
    manifest_provider_tech_ids: set[str] = set()
    for dep in context.manifest_dependencies:
        if dep.technology_id:
            manifest_tech_ids.add(dep.technology_id)
            if dep.provider_id:
                manifest_provider_tech_ids.add(dep.technology_id)

    for technology_id in sorted(manifest_tech_ids):
        dependency_signals += 1
        notes.append(f"ai dependency:{technology_id}")
    manifest_points = min(len(manifest_tech_ids), 3) * 3
    if manifest_points:
        score += manifest_points
        notes.append(f"manifest tech points:{manifest_points}")
    for technology_id in sorted(manifest_provider_tech_ids):
        notes.append(f"provider-specific dependency:{technology_id}")
    provider_points = min(len(manifest_provider_tech_ids), 2) * 2
    if provider_points:
        score += provider_points
        notes.append(f"provider bonus points:{provider_points}")

    sbom_tech_ids: set[str] = set()
    for dep in context.sbom_dependencies:
        if dep.technology_id and dep.technology_id not in manifest_tech_ids:
            sbom_tech_ids.add(dep.technology_id)
    for technology_id in sorted(sbom_tech_ids):
        dependency_signals += 1
        notes.append(f"sbom ai dependency:{technology_id}")
    sbom_points = min(len(sbom_tech_ids), 3) * 2
    if sbom_points:
        score += sbom_points
        notes.append(f"sbom tech points:{sbom_points}")

    seen_import_tech_ids = {
        dep.technology_id
        for dep in context.manifest_dependencies
        + context.sbom_dependencies
        if dep.technology_id
    }
    import_tech_ids: set[str] = set()
    for dep in context.import_dependencies:
        if dep.technology_id and dep.technology_id not in seen_import_tech_ids:
            import_tech_ids.add(dep.technology_id)
    for technology_id in sorted(import_tech_ids):
        dependency_signals += 1
        notes.append(f"import ai dependency:{technology_id}")
    import_points = min(len(import_tech_ids), 2)
    if import_points:
        score += import_points
        notes.append(f"import tech points:{import_points}")

    config_hits = matched_config_keywords(runtime, context.tree_paths)
    if config_hits:
        score += 1
        notes.append(f"ai config signals:{', '.join(sorted(config_hits))}")

    topic_hits = [
        topic
        for topic in repo.topics
        if topic.casefold() in {t.casefold() for t in runtime.discovery.topics}
    ]
    if topic_hits:
        score += 1
        notes.append(f"topic match:{', '.join(sorted(topic_hits))}")

    description_hits = [
        keyword
        for keyword in runtime.discovery.description_keywords
        if keyword.casefold() in (repo.description or "").casefold()
    ]
    if description_hits:
        score += 1
        notes.append(f"description match:{', '.join(sorted(description_hits))}")

    if not dependency_signals and config_hits:
        notes.append("AI relevance inferred from direct repo config")

    return score, notes


def score_segments(runtime: RuntimeConfig, repo: DiscoveredRepo, context: RepoContext) -> list[str]:
    technology_ids = {
        dep.technology_id for dep in context.manifest_dependencies if dep.technology_id
    }
    technology_ids.update(
        dep.technology_id for dep in context.sbom_dependencies if dep.technology_id
    )
    technology_ids.update(
        dep.technology_id for dep in context.import_dependencies if dep.technology_id
    )
    path_blob = "\n".join(context.tree_paths).casefold()
    description = (repo.description or "").casefold()
    topics = {topic.casefold() for topic in repo.topics}
    scores: dict[str, int] = {}

    for rule in runtime.segments.rules:
        value = 0
        value += sum(1 for keyword in rule.topic_keywords if keyword.casefold() in topics)
        value += sum(
            1 for keyword in rule.description_keywords if keyword.casefold() in description
        )
        value += sum(1 for technology_id in rule.technology_ids if technology_id in technology_ids)
        value += sum(1 for keyword in rule.config_keywords if keyword.casefold() in path_blob)
        if value:
            scores[rule.segment_id] = value

    if not scores:
        return []

    precedence = {segment_id: index for index, segment_id in enumerate(runtime.segments.precedence)}
    return [
        segment_id
        for segment_id, _ in sorted(
            scores.items(),
            key=lambda item: (-item[1], precedence.get(item[0], 999)),
        )
    ]


def matched_exclusion_terms(
    runtime: RuntimeConfig, repo: DiscoveredRepo, context: RepoContext
) -> set[str]:
    return set(matched_exclusion_term_sources(runtime, repo))


def matched_exclusion_term_sources(
    runtime: RuntimeConfig,
    repo: DiscoveredRepo,
) -> dict[str, set[str]]:
    haystacks = {
        "full_name": repo.full_name.casefold(),
        "description": (repo.description or "").casefold(),
        "topics": " ".join(repo.topics).casefold(),
    }
    matches: dict[str, set[str]] = {}
    for keyword in runtime.exclusions.hard_keywords:
        sources = {
            source
            for source, haystack in haystacks.items()
            if keyword.casefold() in haystack
        }
        if sources:
            matches[keyword] = sources
    return matches


def can_override_single_exclusion_term(
    term: str,
    sources: set[str],
    context: RepoContext,
) -> bool:
    if sources == {"topics"}:
        return True
    if not has_root_manifest(context.manifest_paths):
        return False
    if term in {"awesome", "list", "course", "tutorial"}:
        return False
    return True


def educational_material_signal(
    runtime: RuntimeConfig,
    repo: DiscoveredRepo,
    readme_text: str,
) -> bool:
    haystack = "\n".join(
        [
            repo.full_name,
            repo.description or "",
            " ".join(repo.topics),
            readme_text[:4000],
        ]
    ).casefold()
    strong_keywords = {
        "official code repository for the book",
        "code for the book",
        "companion code",
        "course materials",
        "textbook",
        "workbook",
        "for demonstration and educational purposes only",
        "provided for demonstration and educational purposes only",
        "curated collection",
        "awesome list",
    }
    weak_keywords = {
        "from scratch",
        "step by step",
        "hands-on",
        "learning materials",
        "chapter code",
    }
    if any(keyword in haystack for keyword in strong_keywords):
        return True
    weak_hits = sum(1 for keyword in weak_keywords if keyword in haystack)
    return weak_hits >= 2


def matched_config_keywords(runtime: RuntimeConfig, tree_paths: list[str]) -> set[str]:
    keywords = {
        "docker-compose",
        "ollama",
        "vllm",
        "qdrant",
        "weaviate",
        "milvus",
        "langgraph",
        "pgvector",
        "chroma",
        "lancedb",
    }
    haystack = "\n".join(tree_paths).casefold()
    return {keyword for keyword in keywords if keyword in haystack}


def find_manifest_paths(tree_paths: list[str], runtime: RuntimeConfig) -> list[str]:
    manifests = []
    excluded_dirs = tuple(
        f"{directory.rstrip('/')}/" for directory in runtime.exclusions.excluded_directories
    )
    allowed_names = set(runtime.exclusions.manifest_files)
    for path in tree_paths:
        if path.startswith(excluded_dirs):
            continue
        name = Path(path).name
        if name in allowed_names or name.endswith((".csproj",)):
            manifests.append(path)
    return manifests


def parse_manifest_dependencies(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    suffix = Path(path).name
    try:
        if suffix == "pyproject.toml":
            return _parse_pyproject(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        if suffix == "requirements.txt":
            return _parse_requirements(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        if suffix == "package.json":
            return _parse_package_json(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        if suffix == "go.mod":
            return _parse_go_mod(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        if suffix == "Cargo.toml":
            return _parse_cargo_toml(
                path,
                text,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
    except Exception:
        return []
    return []


def parse_sbom_dependencies(
    sbom_payload: dict,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    if not sbom_payload:
        return []

    package_by_id = {
        package["SPDXID"]: package
        for package in sbom_payload.get("packages", [])
        if package.get("SPDXID")
    }
    root_id = None
    for relationship in sbom_payload.get("relationships", []):
        if relationship.get("relationshipType") == "DESCRIBES":
            root_id = relationship.get("relatedSpdxElement")
            break
    if not root_id:
        return []

    direct_dependency_ids = {
        relationship.get("relatedSpdxElement")
        for relationship in sbom_payload.get("relationships", [])
        if relationship.get("relationshipType") == "DEPENDS_ON"
        and relationship.get("spdxElementId") == root_id
    }

    dependencies: list[ManifestDependency] = []
    for dependency_id in direct_dependency_ids:
        package = package_by_id.get(dependency_id)
        if not package:
            continue
        purl = extract_purl(package)
        package_candidates = extract_package_candidates_from_sbom(package, purl)
        package_name = normalize_package_name(package_candidates[0])
        match = resolve_package_candidates(
            package_candidates,
            alias_lookup,
            registry_lookup,
            registry_prefix_rules,
        )
        dependencies.append(
            ManifestDependency(
                package_name=package_name,
                dependency_scope="runtime",
                source_path="sbom",
                evidence_type="sbom",
                confidence="high",
                raw_specifier=package.get("versionInfo"),
                raw_version=package.get("versionInfo"),
                purl=purl,
                license_spdx=package.get("licenseConcluded"),
                technology_id=match.technology_id if match else None,
                provider_id=match.provider_id if match else None,
                provider_technology_id=match.provider_technology_id if match else None,
                entity_type=match.entity_type if match else None,
                canonical_product_id=match.canonical_product_id if match else None,
                match_method=match.match_method if match else None,
            )
        )

    return dependencies


def _parse_pyproject(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    data = tomllib.loads(text)
    dependencies: list[ManifestDependency] = []
    for raw_dep in data.get("project", {}).get("dependencies", []):
        name = extract_requirement_name(raw_dep)
        dependencies.append(
            _make_dependency(
                name,
                path,
                raw_dep,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        )
    poetry_deps = data.get("tool", {}).get("poetry", {}).get("dependencies", {})
    for name, spec in poetry_deps.items():
        if name == "python":
            continue
        dependencies.append(
            _make_dependency(
                normalize_package_name(name),
                path,
                str(spec),
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        )
    return dependencies


def _parse_requirements(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    dependencies: list[ManifestDependency] = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#") or cleaned.startswith("-"):
            continue
        name = extract_requirement_name(cleaned)
        dependencies.append(
            _make_dependency(
                name,
                path,
                cleaned,
                alias_lookup,
                registry_lookup,
                registry_prefix_rules,
            )
        )
    return dependencies


def _parse_package_json(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    data = json.loads(text)
    dependencies: list[ManifestDependency] = []
    for scope_name in (
        "dependencies",
        "optionalDependencies",
        "peerDependencies",
        "devDependencies",
    ):
        scope = data.get(scope_name, {})
        for name, spec in scope.items():
            dependency_scope = "dev" if scope_name == "devDependencies" else "runtime"
            dependencies.append(
                _make_dependency(
                    normalize_package_name(name),
                    path,
                    str(spec),
                    alias_lookup,
                    registry_lookup,
                    registry_prefix_rules,
                    dependency_scope,
                )
            )
    return dependencies


def _parse_go_mod(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    dependencies: list[ManifestDependency] = []
    in_require_block = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        if stripped == "require (":
            in_require_block = True
            continue
        if in_require_block and stripped == ")":
            in_require_block = False
            continue
        if stripped.startswith("require "):
            parts = stripped.split()
            if len(parts) < 3:
                continue
            module = parts[1]
            version = parts[2]
        elif in_require_block:
            parts = stripped.split()
            if len(parts) < 2:
                continue
            module = parts[0]
            version = parts[1]
        else:
            continue
        normalized_module = module.split("/")[-1]
        if normalized_module:
            dependencies.append(
                _make_dependency(
                    normalize_package_name(normalized_module),
                    path,
                    version,
                    alias_lookup,
                    registry_lookup,
                    registry_prefix_rules,
                )
            )
    return dependencies


def _parse_cargo_toml(
    path: str,
    text: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> list[ManifestDependency]:
    data = tomllib.loads(text)
    dependencies: list[ManifestDependency] = []
    for section_name in ("dependencies", "build-dependencies", "dev-dependencies"):
        scope = data.get(section_name, {})
        dependency_scope = "dev" if section_name == "dev-dependencies" else "runtime"
        for name, spec in scope.items():
            dependencies.append(
                _make_dependency(
                    normalize_package_name(name),
                    path,
                    str(spec),
                    alias_lookup,
                    registry_lookup,
                    registry_prefix_rules,
                    dependency_scope,
                )
            )
    return dependencies


def _make_dependency(
    package_name: str,
    path: str,
    raw_specifier: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
    dependency_scope: str = "runtime",
) -> ManifestDependency:
    match = resolve_package_match(
        package_name,
        alias_lookup,
        registry_lookup,
        registry_prefix_rules,
    )
    return ManifestDependency(
        package_name=package_name,
        dependency_scope=dependency_scope,
        source_path=path,
        evidence_type="manifest",
        confidence="high",
        raw_specifier=raw_specifier,
        technology_id=match.technology_id if match else None,
        provider_id=match.provider_id if match else None,
        provider_technology_id=match.provider_technology_id if match else None,
        entity_type=match.entity_type if match else None,
        canonical_product_id=match.canonical_product_id if match else None,
        match_method=match.match_method if match else None,
    )


def normalize_package_name(value: str) -> str:
    return value.strip().strip('"').strip("'").casefold()


def extract_requirement_name(raw: str) -> str:
    candidate = raw.split(";", 1)[0]
    candidate = candidate.split("[", 1)[0]
    candidate = re.split(r"[<>=!~ ]", candidate, maxsplit=1)[0]
    return normalize_package_name(candidate)


def dedupe_dependencies(dependencies: list[ManifestDependency]) -> list[ManifestDependency]:
    deduped: dict[tuple[str, str, str], ManifestDependency] = {}
    for dep in dependencies:
        key = (dep.package_name, dep.source_path, dep.dependency_scope)
        existing = deduped.get(key)
        if existing is None or dependency_rank(dep) > dependency_rank(existing):
            deduped[key] = dep
    return list(deduped.values())


def extract_purl(package: dict) -> str | None:
    for ref in package.get("externalRefs", []):
        if ref.get("referenceType") == "purl":
            return ref.get("referenceLocator")
    return None


def extract_package_candidates_from_sbom(package: dict, purl: str | None) -> list[str]:
    candidates: list[str] = []
    if purl and purl.startswith("pkg:"):
        remainder = purl[4:].split("?", 1)[0].split("#", 1)[0]
        if "/" in remainder:
            _, name_part = remainder.split("/", 1)
            decoded = unquote(name_part)
            name = decoded.rsplit("@", 1)[0]
            candidates.append(name)
            if "/" in name:
                basename = name.rsplit("/", 1)[-1]
                if should_include_scoped_basename_candidate(basename):
                    candidates.append(basename)
    package_name = package.get("name")
    if package_name:
        candidates.append(package_name)
        if "/" in package_name:
            basename = package_name.rsplit("/", 1)[-1]
            if should_include_scoped_basename_candidate(basename):
                candidates.append(basename)
    return unique_candidates(candidates)


def should_include_scoped_basename_candidate(candidate: str) -> bool:
    # Scoped package basenames such as "modal" are often generic UI/infra words.
    # Falling back from "@scope/modal" -> "modal" creates false positives during
    # snapshot repair, so keep an explicit blocklist for ambiguous basenames.
    return normalize_package_name(candidate) not in {"modal", "sandbox", "containers"}


def resolve_alias_candidates(
    candidates: list[str],
    alias_lookup: dict[str, TechnologyAlias],
) -> TechnologyAlias | None:
    for candidate in candidates:
        alias = alias_lookup.get(normalize_package_name(candidate))
        if alias is not None:
            return alias
    return None


def resolve_package_candidates(
    candidates: list[str],
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> TechnologyMatch | None:
    for candidate in candidates:
        match = resolve_package_match(
            candidate,
            alias_lookup,
            registry_lookup,
            registry_prefix_rules,
        )
        if match is not None:
            return match
    return None


def resolve_package_match(
    package_name: str,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> TechnologyMatch | None:
    normalized = normalize_package_name(package_name)
    alias = alias_lookup.get(normalized)
    if alias is not None:
        return build_technology_match(alias, match_method="exact_alias")
    registry_alias = registry_lookup.get(normalized)
    if registry_alias is not None:
        provider_alias = infer_provider_alias_from_package(package_name, alias_lookup)
        return build_technology_match(
            registry_alias,
            match_method="registry_alias",
            provider_technology_id=provider_alias.technology_id if provider_alias else None,
        )
    for prefix, technology in registry_prefix_rules:
        if matches_package_prefix(normalized, prefix):
            provider_alias = infer_provider_alias_from_package(package_name, alias_lookup)
            return build_technology_match(
                technology,
                match_method="package_prefix",
                provider_technology_id=provider_alias.technology_id if provider_alias else None,
            )
    return None


def build_technology_match(
    technology: TechnologyAlias,
    *,
    match_method: str,
    provider_technology_id: str | None = None,
) -> TechnologyMatch:
    return TechnologyMatch(
        technology_id=technology.technology_id,
        provider_id=technology.provider_id,
        provider_technology_id=provider_technology_id or technology.technology_id,
        entity_type=technology.entity_type,
        canonical_product_id=technology.canonical_product_id,
        match_method=match_method,
    )


def rebind_package_dependency(
    dependency: ManifestDependency,
    *,
    alias_lookup: dict[str, TechnologyAlias],
    registry_lookup: dict[str, TechnologyAlias],
    registry_prefix_rules: list[tuple[str, TechnologyAlias]],
) -> ManifestDependency:
    package_candidates = extract_package_candidates_from_sbom(
        {"name": dependency.package_name},
        dependency.purl,
    )
    match = resolve_package_candidates(
        package_candidates,
        alias_lookup,
        registry_lookup,
        registry_prefix_rules,
    )
    if match is None:
        return dependency.model_copy(
            update={
                "technology_id": None,
                "provider_id": None,
                "provider_technology_id": None,
                "entity_type": None,
                "canonical_product_id": None,
                "match_method": None,
            }
        )
    return dependency.model_copy(
        update={
            "technology_id": match.technology_id,
            "provider_id": match.provider_id,
            "provider_technology_id": match.provider_technology_id,
            "entity_type": match.entity_type,
            "canonical_product_id": match.canonical_product_id,
            "match_method": match.match_method,
        }
    )


def rebind_import_dependency(
    dependency: ManifestDependency,
    *,
    import_lookup: dict[str, TechnologyAlias],
) -> ManifestDependency:
    alias = resolve_import_alias(dependency.package_name, import_lookup)
    if alias is None:
        return dependency.model_copy(
            update={
                "technology_id": None,
                "provider_id": None,
                "provider_technology_id": None,
                "entity_type": None,
                "canonical_product_id": None,
                "match_method": None,
            }
        )
    return dependency.model_copy(
        update={
            "technology_id": alias.technology_id,
            "provider_id": alias.provider_id,
            "provider_technology_id": alias.technology_id,
            "entity_type": alias.entity_type,
            "canonical_product_id": alias.canonical_product_id,
            "match_method": "import_alias",
        }
    )


def infer_provider_alias_from_package(
    package_name: str,
    alias_lookup: dict[str, TechnologyAlias],
) -> TechnologyAlias | None:
    normalized = normalize_package_name(package_name)
    candidates = [normalized]
    if normalized.startswith("@") and "/" in normalized:
        scope, remainder = normalized[1:].split("/", 1)
        candidates.append(scope)
        candidates.append(remainder)
    if "/" in normalized:
        candidates.append(normalized.rsplit("/", 1)[-1])
    if "-" in normalized:
        candidates.append(normalized.split("-", 1)[0])
        candidates.append(normalized.rsplit("-", 1)[-1])
    if "_" in normalized:
        candidates.append(normalized.split("_", 1)[0])
        candidates.append(normalized.rsplit("_", 1)[-1])
    for candidate in unique_candidates(candidates):
        alias = alias_lookup.get(candidate)
        if alias is not None and alias.provider_id:
            return alias
    return None


def matches_package_prefix(package_name: str, prefix: str) -> bool:
    normalized_package = normalize_package_name(package_name)
    normalized_prefix = normalize_package_name(prefix)
    if normalized_package == normalized_prefix:
        return True
    if normalized_prefix.endswith(("/", "-", "_", ".")):
        return normalized_package.startswith(normalized_prefix)
    return normalized_package.startswith(f"{normalized_prefix}-") or normalized_package.startswith(
        f"{normalized_prefix}_"
    ) or normalized_package.startswith(f"{normalized_prefix}/") or normalized_package.startswith(
        f"{normalized_prefix}."
    )


def unique_candidates(candidates: list[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        normalized = normalize_package_name(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        values.append(candidate)
    return values


def should_run_import_scan(
    runtime: RuntimeConfig,
    repo: DiscoveredRepo,
    manifest_paths: list[str],
    structured_hits: bool,
) -> bool:
    if not manifest_paths:
        return True
    if structured_hits:
        return False
    metadata_text = " ".join([repo.description or "", *repo.topics]).casefold()
    return any(
        keyword.casefold() in metadata_text
        for keyword in runtime.discovery.topics + runtime.discovery.description_keywords
    )


def dependency_rank(dep: ManifestDependency) -> tuple[int, int]:
    evidence_priority = {"manifest": 3, "sbom": 2}
    scope_priority = {"runtime": 2, "optional": 1, "dev": 0}
    return (
        evidence_priority.get(dep.evidence_type, 0),
        scope_priority.get(dep.dependency_scope, 0),
    )


def has_code_footprint(tree_paths: list[str], runtime: RuntimeConfig) -> bool:
    valid = [
        path
        for path in tree_paths
        if Path(path).suffix in runtime.exclusions.source_extensions
        and not any(
            path.startswith(f"{directory.rstrip('/')}/")
            for directory in runtime.exclusions.excluded_directories
        )
    ]
    return len(valid) >= 5


def monorepo_subproject_count(tree_paths: list[str]) -> int:
    roots = {path.split("/", 1)[0] for path in tree_paths if "/" in path}
    return len([root for root in roots if root in {"packages", "apps", "services"}])


def readme_looks_like_product_docs(readme_text: str) -> bool:
    lowered = readme_text.casefold()
    return "installation" in lowered or "quickstart" in lowered or "usage" in lowered


def has_root_manifest(manifest_paths: list[str]) -> bool:
    return any("/" not in path for path in manifest_paths)


def mostly_notebooks_or_docs(tree_paths: list[str]) -> bool:
    if not tree_paths:
        return False
    doc_like = sum(
        1
        for path in tree_paths
        if path.endswith((".md", ".ipynb", ".png", ".jpg", ".jpeg", ".pdf"))
    )
    return doc_like / len(tree_paths) >= 0.7


def code_only_in_examples(tree_paths: list[str], runtime: RuntimeConfig) -> bool:
    source_paths = [
        path for path in tree_paths if Path(path).suffix in runtime.exclusions.source_extensions
    ]
    if not source_paths:
        return False
    return all(
        path.startswith(("examples/", "example/", "demo/", "tutorials/", "tutorial/"))
        for path in source_paths
    )


def only_prompt_material(tree_paths: list[str]) -> bool:
    if not tree_paths:
        return False
    prompt_like = sum(
        1 for path in tree_paths if "prompt" in path.casefold() or path.endswith(".ipynb")
    )
    return prompt_like / len(tree_paths) >= 0.5


def safe_call(fn, default, reraise_status_codes: set[int] | None = None):
    try:
        return fn()
    except Exception as exc:
        if should_reraise_safe_call_exception(
            exc,
            reraise_status_codes=reraise_status_codes,
        ):
            raise
        return default


def should_reraise_safe_call_exception(
    exc: Exception,
    *,
    reraise_status_codes: set[int] | None = None,
) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        effective_status_codes = reraise_status_codes or {403, 429, 500, 502, 503, 504}
        return status_code in effective_status_codes
    return False
