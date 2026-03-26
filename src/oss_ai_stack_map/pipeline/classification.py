from __future__ import annotations

import json
import re
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import unquote

from oss_ai_stack_map.config.loader import RuntimeConfig, TechnologyAlias
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    ClassificationSummary,
    DiscoveredRepo,
    JudgeDecision,
    ManifestDependency,
    RepoContext,
    StageTiming,
)
from oss_ai_stack_map.openai.judge import OpenAIJudge
from oss_ai_stack_map.pipeline.imports import collect_import_dependencies
from oss_ai_stack_map.pipeline.normalize import build_repo_technology_edges, build_technology_rows
from oss_ai_stack_map.storage.checkpoints import ClassificationCheckpointStore
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows


@dataclass
class JudgeCandidate:
    decision: ClassificationDecision
    judge_mode: str


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
    processed_repo_count = len(completed_repo_ids)
    batch_index = checkpoint_store.next_batch_index()

    for index, repo in enumerate(repos, start=1):
        context = build_repo_context(
            runtime=runtime, client=client, repo=repo, alias_lookup=alias_lookup
        )
        decision = classify_repo(
            runtime=runtime, repo=repo, context=context, alias_lookup=alias_lookup
        )
        batch_contexts.append(context)
        batch_decisions.append(decision)
        if len(batch_contexts) >= runtime.study.checkpoint_batch_size or index == len(repos):
            flush_classification_checkpoint_batch(
                checkpoint_store=checkpoint_store,
                runtime=runtime,
                contexts=batch_contexts,
                decisions=batch_decisions,
                batch_index=batch_index,
            )
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
        if progress and (index <= 3 or index == len(repos) or index % 50 == 0):
            progress(
                "classification: "
                f"processed {processed_repo_count + len(batch_contexts)}/{total_repos} repos "
                f"(latest: {repo.full_name})"
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

    judge_candidates = select_judge_candidates(
        runtime=runtime,
        decisions=decisions,
        already_judged_repo_ids={decision.repo_id for decision in existing_judge_decisions},
    )
    judge_contexts = checkpoint_store.read_checkpoint_models_for_repo_ids(
        "repo_contexts",
        RepoContext,
        {candidate.decision.repo_id for candidate in judge_candidates},
    )
    judge_started_at = time.perf_counter()
    judge_decisions = maybe_run_judge(
        runtime=runtime,
        repos_by_id={repo.repo_id: repo for repo in all_repos},
        contexts_by_id={context.repo_id: context for context in judge_contexts},
        candidates=judge_candidates,
    )
    judge_seconds = time.perf_counter() - judge_started_at
    if judge_decisions:
        apply_judge_decisions(runtime=runtime, decisions=decisions, judge_decisions=judge_decisions)
    all_judge_decisions = merge_judge_decisions(existing_judge_decisions, judge_decisions)

    write_started_at = time.perf_counter()
    contexts = checkpoint_store.read_checkpoint_models("repo_contexts", RepoContext)
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
            notes="repo readme/tree/manifest/sbom/import processing",
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


def select_judge_candidates(
    runtime: RuntimeConfig,
    decisions: list[ClassificationDecision],
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
        validation_candidates = [
            JudgeCandidate(decision=decision, judge_mode="validation")
            for decision in decisions
            if decision.repo_id not in selected_repo_ids
            and should_send_to_validation_judge(decision)
        ][: max_cases - len(hardening_candidates)]
    return [*hardening_candidates, *validation_candidates]


def should_send_to_validation_judge(decision: ClassificationDecision) -> bool:
    return decision.passed_major_filter


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

        decision.rule_passed_serious_filter = decision.passed_serious_filter
        decision.rule_passed_ai_relevance_filter = decision.passed_ai_relevance_filter
        decision.rule_passed_major_filter = decision.passed_major_filter
        decision.judge_applied = True
        decision.judge_mode = judge.judge_mode
        decision.judge_confidence = judge.confidence
        decision.judge_include_in_final_set = judge.include_in_final_set
        decision.judge_serious_project = judge.serious_project
        decision.judge_ai_relevant = judge.ai_relevant
        decision.judge_primary_segment = judge.primary_segment
        decision.judge_reasons = judge.reasons

        judge_changes_rule = (
            decision.passed_serious_filter != judge.serious_project
            or decision.passed_ai_relevance_filter != judge.ai_relevant
            or decision.passed_major_filter != judge.include_in_final_set
            or (
                judge.primary_segment is not None
                and decision.primary_segment != judge.primary_segment
            )
        )
        hardening_is_authoritative = (
            judge.judge_mode == "hardening"
            and runtime.study.judge.override_on_high_confidence
            and confidence_order.get(judge.confidence, 0) >= min_confidence
        )
        validation_is_authoritative = judge.judge_mode == "validation"
        judge_is_authoritative = validation_is_authoritative or hardening_is_authoritative

        if not judge_is_authoritative:
            decision.notes.append(f"{judge.judge_mode} judge reviewed without override")
            continue

        decision.passed_serious_filter = judge.serious_project
        decision.passed_ai_relevance_filter = judge.ai_relevant
        decision.passed_major_filter = judge.include_in_final_set
        if judge.primary_segment:
            decision.primary_segment = judge.primary_segment
        if judge_changes_rule:
            decision.judge_override_applied = True
            decision.notes.append(f"{judge.judge_mode} judge override applied")
        else:
            decision.notes.append(f"{judge.judge_mode} judge reviewed without override")


def build_repo_context(
    runtime: RuntimeConfig,
    client: GitHubClient,
    repo: DiscoveredRepo,
    alias_lookup: dict[str, TechnologyAlias],
) -> RepoContext:
    owner, name = repo.full_name.split("/", 1)
    readme_text = safe_call(lambda: client.get_readme(owner, name), default="")
    tree_paths = safe_call(lambda: client.get_tree(owner, name), default=[])
    manifest_paths = find_manifest_paths(tree_paths, runtime)
    import_lookup = runtime.aliases.import_lookup()

    manifest_dependencies: list[ManifestDependency] = []
    for path in manifest_paths:
        text = safe_call(lambda path=path: client.get_file_text(owner, name, path), default="")
        manifest_dependencies.extend(parse_manifest_dependencies(path, text, alias_lookup))
    sbom_payload = safe_call(lambda: client.get_sbom(owner, name), default={})
    sbom_dependencies = parse_sbom_dependencies(sbom_payload, alias_lookup)
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
        readme_text=readme_text,
        tree_paths=tree_paths,
        manifest_paths=manifest_paths,
        manifest_dependencies=dedupe_dependencies(manifest_dependencies),
        sbom_dependencies=dedupe_dependencies(sbom_dependencies),
        import_dependencies=dedupe_dependencies(import_dependencies),
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
) -> list[ManifestDependency]:
    suffix = Path(path).name
    try:
        if suffix == "pyproject.toml":
            return _parse_pyproject(path, text, alias_lookup)
        if suffix == "requirements.txt":
            return _parse_requirements(path, text, alias_lookup)
        if suffix == "package.json":
            return _parse_package_json(path, text, alias_lookup)
        if suffix == "go.mod":
            return _parse_go_mod(path, text, alias_lookup)
        if suffix == "Cargo.toml":
            return _parse_cargo_toml(path, text, alias_lookup)
    except Exception:
        return []
    return []


def parse_sbom_dependencies(
    sbom_payload: dict,
    alias_lookup: dict[str, TechnologyAlias],
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
        alias = resolve_alias_candidates(package_candidates, alias_lookup)
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
                technology_id=alias.technology_id if alias else None,
                provider_id=alias.provider_id if alias else None,
            )
        )

    return dependencies


def _parse_pyproject(
    path: str, text: str, alias_lookup: dict[str, TechnologyAlias]
) -> list[ManifestDependency]:
    data = tomllib.loads(text)
    dependencies: list[ManifestDependency] = []
    for raw_dep in data.get("project", {}).get("dependencies", []):
        name = extract_requirement_name(raw_dep)
        dependencies.append(_make_dependency(name, path, raw_dep, alias_lookup))
    poetry_deps = data.get("tool", {}).get("poetry", {}).get("dependencies", {})
    for name, spec in poetry_deps.items():
        if name == "python":
            continue
        dependencies.append(
            _make_dependency(normalize_package_name(name), path, str(spec), alias_lookup)
        )
    return dependencies


def _parse_requirements(
    path: str, text: str, alias_lookup: dict[str, TechnologyAlias]
) -> list[ManifestDependency]:
    dependencies: list[ManifestDependency] = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#") or cleaned.startswith("-"):
            continue
        name = extract_requirement_name(cleaned)
        dependencies.append(_make_dependency(name, path, cleaned, alias_lookup))
    return dependencies


def _parse_package_json(
    path: str, text: str, alias_lookup: dict[str, TechnologyAlias]
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
                    normalize_package_name(name), path, str(spec), alias_lookup, dependency_scope
                )
            )
    return dependencies


def _parse_go_mod(
    path: str, text: str, alias_lookup: dict[str, TechnologyAlias]
) -> list[ManifestDependency]:
    dependencies: list[ManifestDependency] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("require "):
            continue
        parts = stripped.split()
        if len(parts) >= 3:
            module = parts[1].split("/")[-1]
            dependencies.append(
                _make_dependency(normalize_package_name(module), path, parts[2], alias_lookup)
            )
    return dependencies


def _parse_cargo_toml(
    path: str, text: str, alias_lookup: dict[str, TechnologyAlias]
) -> list[ManifestDependency]:
    data = tomllib.loads(text)
    dependencies: list[ManifestDependency] = []
    for section_name in ("dependencies", "build-dependencies", "dev-dependencies"):
        scope = data.get(section_name, {})
        dependency_scope = "dev" if section_name == "dev-dependencies" else "runtime"
        for name, spec in scope.items():
            dependencies.append(
                _make_dependency(
                    normalize_package_name(name), path, str(spec), alias_lookup, dependency_scope
                )
            )
    return dependencies


def _make_dependency(
    package_name: str,
    path: str,
    raw_specifier: str,
    alias_lookup: dict[str, TechnologyAlias],
    dependency_scope: str = "runtime",
) -> ManifestDependency:
    alias = alias_lookup.get(package_name.casefold())
    return ManifestDependency(
        package_name=package_name,
        dependency_scope=dependency_scope,
        source_path=path,
        evidence_type="manifest",
        confidence="high",
        raw_specifier=raw_specifier,
        technology_id=alias.technology_id if alias else None,
        provider_id=alias.provider_id if alias else None,
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
                candidates.append(name.rsplit("/", 1)[-1])
    package_name = package.get("name")
    if package_name:
        candidates.append(package_name)
        if "/" in package_name:
            candidates.append(package_name.rsplit("/", 1)[-1])
    return unique_candidates(candidates)


def resolve_alias_candidates(
    candidates: list[str],
    alias_lookup: dict[str, TechnologyAlias],
) -> TechnologyAlias | None:
    for candidate in candidates:
        alias = alias_lookup.get(normalize_package_name(candidate))
        if alias is not None:
            return alias
    return None


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


def safe_call(fn, default):
    try:
        return fn()
    except Exception:
        return default
