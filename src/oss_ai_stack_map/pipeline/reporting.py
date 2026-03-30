from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.pipeline.anchors import llm_anchor_technology_ids
from oss_ai_stack_map.pipeline.technology_discovery import package_has_ai_affinity

NOISY_UNMATCHED_PACKAGE_PREFIXES = (
    "@types/",
    "@esbuild/",
    "@radix-ui/",
    "react-",
    "eslint-",
)
NOISY_UNMATCHED_PACKAGE_NAMES = {
    "react",
    "react-dom",
    "eslint",
}
COMMODITY_UNMATCHED_PACKAGE_PREFIXES = {
    "@rollup/",
    "@types/",
    "eslint",
    "github",
    "opentelemetry",
    "python",
    "react",
    "requests",
    "rollup",
    "serde",
    "tailwind",
    "typescript",
}


@dataclass
class GapReport:
    final_repos_missing_edges_count: int
    final_repos_missing_edges_with_unmapped_dependency_evidence_count: int
    final_repos_missing_edges_with_no_dependency_evidence_count: int
    final_repos_missing_edges: list[dict[str, Any]]
    top_unmatched_packages: list[dict[str, Any]]
    top_unmatched_package_prefixes: list[dict[str, Any]]
    top_ai_specific_unmatched_package_prefixes: list[dict[str, Any]]
    top_commodity_unmatched_package_prefixes: list[dict[str, Any]]
    top_vendor_like_unmapped_repos: list[dict[str, Any]]
    suggested_discovery_inputs: list[dict[str, Any]]


@dataclass
class BenchmarkRecallReport:
    total_entity_count: int
    entity_count: int
    negative_entity_count: int
    tuning_entity_count: int
    holdout_entity_count: int
    entities_with_repo_discovered: int
    entities_with_repo_discovered_by_anchor: int
    entities_with_repo_discovered_by_search: int
    entities_with_repo_discovered_by_seed_only: int
    entities_with_repo_included: int
    entities_with_repo_identity_mapped: int
    entities_with_third_party_adoption: int
    entities_with_dependency_evidence: int
    repo_discovered_rate: float
    repo_discovered_by_anchor_rate: float
    repo_discovered_by_search_rate: float
    repo_discovered_by_seed_only_rate: float
    repo_included_rate: float
    repo_identity_mapped_rate: float
    third_party_adoption_rate: float
    dependency_evidence_rate: float
    negative_repo_excluded_rate: float
    negative_repo_discovered_rate: float
    holdout_repo_discovered_rate: float
    holdout_repo_included_rate: float
    thresholds: dict[str, Any]
    failed_thresholds: list[dict[str, Any]]
    prioritized_gaps: list[dict[str, Any]]
    entities: list[dict[str, Any]]


@dataclass
class EvidenceTierReport:
    final_repo_count: int
    direct_supported_repo_count: int
    fallback_supported_repo_count: int
    unmapped_final_repo_count: int
    readme_only_repo_count: int
    repo_identity_only_repo_count: int
    mixed_fallback_repo_count: int
    tiers: list[dict[str, Any]]
    repo_evidence_profiles: list[dict[str, Any]]


@dataclass
class ValidationAuditReport:
    sample_count: int
    changed_decision_count: int
    exclusion_count: int
    inclusion_change_rate: float
    estimated_false_positive_rate: float
    estimated_false_positive_rate_ci95: dict[str, float]
    profile_counts: list[dict[str, Any]]
    segment_counts: list[dict[str, Any]]
    ai_score_band_counts: list[dict[str, Any]]
    serious_score_band_counts: list[dict[str, Any]]
    changed_repos: list[dict[str, Any]]
    sampled_repos: list[dict[str, Any]]


@dataclass
class RobustnessReport:
    rule_only: dict[str, Any]
    judge_adjusted: dict[str, Any]
    evidence_tiers: dict[str, Any]
    temporal_comparison: dict[str, Any] | None


@dataclass
class ReviewQueueReport:
    total_review_item_count: int
    missing_edge_final_count: int
    readme_only_final_count: int
    audit_changed_repo_count: int
    benchmark_priority_gap_count: int
    missing_edge_finals: list[dict[str, Any]]
    readme_only_finals: list[dict[str, Any]]
    audit_changed_repos: list[dict[str, Any]]
    benchmark_priority_gaps: list[dict[str, Any]]
    prioritized_review_items: list[dict[str, Any]]


@dataclass
class ReportSummary:
    total_repos: int
    serious_repos: int
    ai_relevant_repos: int
    final_repos: int
    top_technologies: list[dict]
    top_providers: list[dict]
    gap_report: GapReport
    benchmark_recall_report: BenchmarkRecallReport | None = None
    evidence_tier_report: EvidenceTierReport | None = None
    validation_audit_report: ValidationAuditReport | None = None
    robustness_report: RobustnessReport | None = None


def build_report_summary(
    input_dir: Path,
    top_n: int = 10,
    runtime: RuntimeConfig | None = None,
) -> ReportSummary:
    decisions = _read_if_exists(input_dir / "repo_inclusion_decisions.parquet")
    edges = _read_if_exists(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "technology_id", "provider_id"],
    )
    technologies = {
        row["technology_id"]: row
        for row in _read_if_exists(
            input_dir / "technologies.parquet",
            columns=["technology_id", "display_name", "category_id"],
        )
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

    gap_report = build_gap_report(input_dir=input_dir, top_n=top_n)
    evidence_tier_report = build_evidence_tier_report(input_dir=input_dir, top_n=top_n)
    validation_audit_report = build_validation_audit_report(input_dir=input_dir, top_n=top_n)
    robustness_report = build_robustness_report(
        input_dir=input_dir,
        evidence_tier_report=evidence_tier_report,
    )
    benchmark_recall_report = (
        build_benchmark_recall_report(input_dir=input_dir, runtime=runtime)
        if runtime is not None and runtime.benchmarks.entities
        else None
    )

    return ReportSummary(
        total_repos=total_repos,
        serious_repos=serious_repos,
        ai_relevant_repos=ai_relevant_repos,
        final_repos=final_repos,
        top_technologies=top_technologies,
        top_providers=top_providers,
        gap_report=gap_report,
        benchmark_recall_report=benchmark_recall_report,
        evidence_tier_report=evidence_tier_report,
        validation_audit_report=validation_audit_report,
        robustness_report=robustness_report,
    )


def build_gap_report(input_dir: Path, top_n: int = 10) -> GapReport:
    repos = _read_if_exists(
        input_dir / "repos.parquet",
    )
    repo_by_id = {row["repo_id"]: row for row in repos}
    decisions = _read_if_exists(
        input_dir / "repo_inclusion_decisions.parquet",
    )
    edge_repo_ids = {
        row["repo_id"]
        for row in _read_if_exists(input_dir / "repo_technology_edges.parquet", columns=["repo_id"])
    }
    technologies = _read_if_exists(
        input_dir / "technologies.parquet",
    )
    evidence_rows = _load_dependency_evidence_rows(input_dir=input_dir, repo_by_id=repo_by_id)
    evidence_repo_ids = {row["repo_id"] for row in evidence_rows if row.get("repo_id") is not None}
    mapped_evidence_repo_ids = {
        row["repo_id"]
        for row in evidence_rows
        if row.get("repo_id") is not None and row.get("technology_id")
    }
    unmatched_evidence_counts = Counter()
    for row in evidence_rows:
        if row.get("repo_id") is None or row.get("technology_id"):
            continue
        package_name = row.get("package_name")
        if not package_name or is_noisy_unmatched_package(package_name):
            continue
        unmatched_evidence_counts[row["repo_id"]] += 1

    final_rows = [row for row in decisions if row.get("passed_major_filter")]
    missing_edge_rows = []
    for row in final_rows:
        if row["repo_id"] in edge_repo_ids:
            continue
        repo = repo_by_id.get(row["repo_id"], {})
        has_dependency_evidence = row["repo_id"] in evidence_repo_ids
        gap_reason = (
            "unmapped_dependency_evidence"
            if has_dependency_evidence and row["repo_id"] not in mapped_evidence_repo_ids
            else "no_dependency_evidence"
        )
        missing_edge_rows.append(
            {
                "repo_id": row["repo_id"],
                "full_name": (
                    row.get("full_name")
                    or repo.get("full_name")
                    or f"repo-{row['repo_id']}"
                ),
                "stars": repo.get("stars", 0),
                "gap_reason": gap_reason,
                "unmatched_dependency_count": unmatched_evidence_counts.get(row["repo_id"], 0),
            }
        )
    missing_edge_rows.sort(
        key=lambda row: (
            row["gap_reason"] != "unmapped_dependency_evidence",
            -row.get("unmatched_dependency_count", 0),
            -row.get("stars", 0),
            row["full_name"],
        )
    )
    unmatched_packages = Counter()
    unmatched_prefixes = Counter()
    prefix_ai_occurrences = Counter()
    for row in evidence_rows:
        if row.get("technology_id"):
            continue
        package_name = row.get("package_name")
        if not package_name:
            continue
        if is_noisy_unmatched_package(package_name):
            continue
        unmatched_packages[package_name] += 1
        package_prefix = infer_package_prefix(package_name)
        unmatched_prefixes[package_prefix] += 1
        if package_has_ai_affinity(package_name):
            prefix_ai_occurrences[package_prefix] += 1

    ai_specific_prefixes = Counter(
        {
            prefix: count
            for prefix, count in unmatched_prefixes.items()
            if is_ai_specific_unmatched_prefix(
                prefix,
                ai_occurrence_count=prefix_ai_occurrences.get(prefix, 0),
            )
        }
    )
    commodity_prefixes = Counter(
        {
            prefix: count
            for prefix, count in unmatched_prefixes.items()
            if is_commodity_unmatched_prefix(
                prefix,
                ai_occurrence_count=prefix_ai_occurrences.get(prefix, 0),
            )
        }
    )

    repo_name_lookup = {
        repo_name.casefold()
        for row in technologies
        for repo_name in row.get("repo_names", []) or []
    }
    vendor_terms = build_vendor_terms(technologies)
    vendor_like_unmapped = []
    for row in repos:
        full_name = row["full_name"]
        if full_name.casefold() in repo_name_lookup:
            continue
        matched_terms = sorted(find_vendor_terms(row=row, vendor_terms=vendor_terms))
        if not matched_terms:
            continue
        vendor_like_unmapped.append(
            {
                "repo_id": row["repo_id"],
                "full_name": full_name,
                "stars": row.get("stars", 0),
                "matched_terms": matched_terms,
            }
        )
    vendor_like_unmapped.sort(key=lambda row: (-row.get("stars", 0), row["full_name"]))
    suggested_discovery_inputs = build_suggested_discovery_inputs(
        ai_specific_prefixes=ai_specific_prefixes,
        vendor_like_unmapped=vendor_like_unmapped,
        top_n=top_n,
    )

    return GapReport(
        final_repos_missing_edges_count=len(missing_edge_rows),
        final_repos_missing_edges_with_unmapped_dependency_evidence_count=sum(
            1 for row in missing_edge_rows if row["gap_reason"] == "unmapped_dependency_evidence"
        ),
        final_repos_missing_edges_with_no_dependency_evidence_count=sum(
            1 for row in missing_edge_rows if row["gap_reason"] == "no_dependency_evidence"
        ),
        final_repos_missing_edges=missing_edge_rows[:top_n],
        top_unmatched_packages=[
            {"package_name": package_name, "count": count}
            for package_name, count in unmatched_packages.most_common(top_n)
        ],
        top_unmatched_package_prefixes=[
            {"package_prefix": prefix, "count": count}
            for prefix, count in unmatched_prefixes.most_common(top_n)
        ],
        top_ai_specific_unmatched_package_prefixes=[
            {"package_prefix": prefix, "count": count}
            for prefix, count in ai_specific_prefixes.most_common(top_n)
        ],
        top_commodity_unmatched_package_prefixes=[
            {"package_prefix": prefix, "count": count}
            for prefix, count in commodity_prefixes.most_common(top_n)
        ],
        top_vendor_like_unmapped_repos=vendor_like_unmapped[:top_n],
        suggested_discovery_inputs=suggested_discovery_inputs,
    )


DIRECT_EDGE_TYPES = {"manifest", "sbom", "import", "repo_identity"}


def build_evidence_tier_report(input_dir: Path, top_n: int = 10) -> EvidenceTierReport:
    decisions = _read_if_exists(
        input_dir / "repo_inclusion_decisions.parquet",
        columns=["repo_id", "full_name", "passed_major_filter"],
    )
    edges = _read_if_exists(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "technology_id", "evidence_type"],
    )
    final_rows = [row for row in decisions if row.get("passed_major_filter")]
    final_repo_ids = {row["repo_id"] for row in final_rows}

    fallback_pairs: set[tuple[int, str]] = set()
    direct_pairs: set[tuple[int, str]] = set()
    repo_edge_types: dict[int, set[str]] = {}
    for row in edges:
        if row["repo_id"] not in final_repo_ids:
            continue
        pair = (row["repo_id"], row["technology_id"])
        repo_edge_types.setdefault(row["repo_id"], set()).add(row.get("evidence_type") or "unknown")
        fallback_pairs.add(pair)
        if row.get("evidence_type") in DIRECT_EDGE_TYPES:
            direct_pairs.add(pair)

    profile_rows: list[dict[str, Any]] = []
    profile_counts: Counter[str] = Counter()
    for row in final_rows:
        evidence_types = repo_edge_types.get(row["repo_id"], set())
        profile = classify_repo_evidence_profile(evidence_types)
        profile_counts[profile] += 1
        profile_rows.append(
            {
                "repo_id": row["repo_id"],
                "full_name": row.get("full_name"),
                "evidence_profile": profile,
            }
        )

    direct_supported_repo_ids = {
        repo_id
        for repo_id, evidence_types in repo_edge_types.items()
        if evidence_types & DIRECT_EDGE_TYPES
    }
    fallback_supported_repo_ids = set(repo_edge_types)
    repo_identity_only_repo_count = profile_counts.get("repo_identity_only", 0)
    readme_only_repo_count = profile_counts.get("readme_only", 0)
    mixed_fallback_repo_count = sum(
        profile_counts.get(profile, 0)
        for profile in ("mixed_direct_and_fallback", "repo_identity_plus_fallback")
    )

    tiers = [
        {
            "tier_id": "direct_only",
            "label": "Direct only",
            "repo_count": len(direct_supported_repo_ids),
            "repo_share": ratio(len(direct_supported_repo_ids), len(final_rows)),
            "edge_count": len(direct_pairs),
        },
        {
            "tier_id": "reviewed_fallback",
            "label": "Reviewed fallback",
            "repo_count": len(fallback_supported_repo_ids),
            "repo_share": ratio(len(fallback_supported_repo_ids), len(final_rows)),
            "edge_count": len(fallback_pairs),
        },
        {
            "tier_id": "full_final_population",
            "label": "Full final population",
            "repo_count": len(final_rows),
            "repo_share": ratio(len(final_rows), len(final_rows)),
            "edge_count": len(fallback_pairs),
        },
    ]

    return EvidenceTierReport(
        final_repo_count=len(final_rows),
        direct_supported_repo_count=len(direct_supported_repo_ids),
        fallback_supported_repo_count=len(fallback_supported_repo_ids),
        unmapped_final_repo_count=len(final_rows) - len(fallback_supported_repo_ids),
        readme_only_repo_count=readme_only_repo_count,
        repo_identity_only_repo_count=repo_identity_only_repo_count,
        mixed_fallback_repo_count=mixed_fallback_repo_count,
        tiers=tiers,
        repo_evidence_profiles=[
            {"evidence_profile": profile, "repo_count": count}
            for profile, count in profile_counts.most_common(top_n)
        ],
    )


def build_validation_audit_report(input_dir: Path, top_n: int = 10) -> ValidationAuditReport | None:
    judge_rows = _read_if_exists(
        input_dir / "judge_decisions.parquet",
        columns=[
            "repo_id",
            "full_name",
            "judge_mode",
            "applied",
            "include_in_final_set",
            "confidence",
        ],
    )
    validation_rows = [row for row in judge_rows if row.get("judge_mode") == "validation"]
    if not validation_rows:
        return None

    repo_rows = {
        row["repo_id"]: row
        for row in _read_if_exists(
            input_dir / "repos.parquet",
            columns=["repo_id", "full_name", "stars"],
        )
    }
    decisions = {
        row["repo_id"]: row
        for row in _read_if_exists(
            input_dir / "repo_inclusion_decisions.parquet",
            columns=[
                "repo_id",
                "full_name",
                "score_ai",
                "score_serious",
                "primary_segment",
                "passed_major_filter",
                "rule_passed_major_filter",
                "judge_override_applied",
            ],
        )
    }
    edges = _read_if_exists(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "evidence_type"],
    )
    repo_edge_types: dict[int, set[str]] = {}
    for row in edges:
        repo_edge_types.setdefault(row["repo_id"], set()).add(row.get("evidence_type") or "unknown")

    profile_counts: Counter[str] = Counter()
    segment_counts: Counter[str] = Counter()
    ai_score_band_counts: Counter[str] = Counter()
    serious_score_band_counts: Counter[str] = Counter()
    changed_rows: list[dict[str, Any]] = []
    sampled_rows: list[dict[str, Any]] = []
    exclusion_count = 0

    for row in validation_rows:
        decision = decisions.get(row["repo_id"], {})
        repo = repo_rows.get(row["repo_id"], {})
        profile = classify_repo_evidence_profile(repo_edge_types.get(row["repo_id"], set()))
        segment = decision.get("primary_segment") or "unassigned"
        ai_band = score_band(decision.get("score_ai"))
        serious_band = score_band(decision.get("score_serious"))
        profile_counts[profile] += 1
        segment_counts[segment] += 1
        ai_score_band_counts[ai_band] += 1
        serious_score_band_counts[serious_band] += 1
        changed = bool(row.get("applied"))
        excluded = changed and not row.get("include_in_final_set", True)
        exclusion_count += int(excluded)
        sample_row = {
            "repo_id": row["repo_id"],
            "full_name": row.get("full_name") or decision.get("full_name"),
            "stars": int(repo.get("stars", 0) or 0),
            "primary_segment": segment,
            "evidence_profile": profile,
            "score_ai_band": ai_band,
            "score_serious_band": serious_band,
            "judge_confidence": row.get("confidence"),
            "validation_changed_decision": changed,
            "final_included": bool(decision.get("passed_major_filter")),
            "rule_included": bool(decision.get("rule_passed_major_filter")),
            "prior_override_retained": bool(decision.get("judge_override_applied")),
        }
        sampled_rows.append(sample_row)
        if changed:
            changed_rows.append(sample_row)

    sample_count = len(validation_rows)
    changed_count = len(changed_rows)
    exclusion_rate = ratio(exclusion_count, sample_count)
    ci_low, ci_high = wilson_interval(exclusion_count, sample_count)

    return ValidationAuditReport(
        sample_count=sample_count,
        changed_decision_count=changed_count,
        exclusion_count=exclusion_count,
        inclusion_change_rate=ratio(changed_count, sample_count),
        estimated_false_positive_rate=exclusion_rate,
        estimated_false_positive_rate_ci95={"lower": round(ci_low, 4), "upper": round(ci_high, 4)},
        profile_counts=[
            {"evidence_profile": key, "repo_count": value}
            for key, value in profile_counts.most_common(top_n)
        ],
        segment_counts=[
            {"primary_segment": key, "repo_count": value}
            for key, value in segment_counts.most_common(top_n)
        ],
        ai_score_band_counts=[
            {"score_ai_band": key, "repo_count": value}
            for key, value in ai_score_band_counts.most_common(top_n)
        ],
        serious_score_band_counts=[
            {"score_serious_band": key, "repo_count": value}
            for key, value in serious_score_band_counts.most_common(top_n)
        ],
        changed_repos=sorted(changed_rows, key=lambda row: (-row["stars"], row["full_name"]))[:top_n],
        sampled_repos=sorted(sampled_rows, key=lambda row: (-row["stars"], row["full_name"]))[: max(top_n, 20)],
    )


def build_robustness_report(
    input_dir: Path,
    evidence_tier_report: EvidenceTierReport | None = None,
) -> RobustnessReport:
    decisions = _read_if_exists(
        input_dir / "repo_inclusion_decisions.parquet",
        columns=["passed_major_filter", "rule_passed_major_filter"],
    )
    edges = _read_if_exists(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "technology_id", "evidence_type"],
    )
    evidence_tier_report = evidence_tier_report or build_evidence_tier_report(input_dir=input_dir, top_n=10)
    rule_only_final_repo_count = sum(1 for row in decisions if row.get("rule_passed_major_filter"))
    judge_adjusted_final_repo_count = sum(1 for row in decisions if row.get("passed_major_filter"))
    direct_edge_pairs = {
        (row["repo_id"], row["technology_id"])
        for row in edges
        if row.get("evidence_type") in DIRECT_EDGE_TYPES
    }
    judge_changed_final_repo_count = sum(
        1
        for row in decisions
        if row.get("rule_passed_major_filter") != row.get("passed_major_filter")
    )

    temporal_comparison = None
    baseline_dir = load_temporal_baseline_dir(input_dir)
    if baseline_dir is not None and baseline_dir.exists():
        baseline_evidence = build_evidence_tier_report(input_dir=baseline_dir, top_n=10)
        baseline_decisions = _read_if_exists(
            baseline_dir / "repo_inclusion_decisions.parquet",
            columns=["passed_major_filter"],
        )
        temporal_comparison = {
            "baseline_snapshot_dir": str(baseline_dir),
            "final_repo_delta": judge_adjusted_final_repo_count
            - sum(1 for row in baseline_decisions if row.get("passed_major_filter")),
            "mapped_repo_delta": evidence_tier_report.fallback_supported_repo_count
            - baseline_evidence.fallback_supported_repo_count,
            "readme_only_repo_delta": evidence_tier_report.readme_only_repo_count
            - baseline_evidence.readme_only_repo_count,
        }

    return RobustnessReport(
        rule_only={
            "final_repo_count": rule_only_final_repo_count,
        },
        judge_adjusted={
            "final_repo_count": judge_adjusted_final_repo_count,
            "judge_changed_final_repo_count": judge_changed_final_repo_count,
        },
        evidence_tiers={
            "direct_supported_repo_count": evidence_tier_report.direct_supported_repo_count,
            "fallback_supported_repo_count": evidence_tier_report.fallback_supported_repo_count,
            "unmapped_final_repo_count": evidence_tier_report.unmapped_final_repo_count,
            "direct_edge_count": len(direct_edge_pairs),
            "fallback_edge_count": next(
                (tier["edge_count"] for tier in evidence_tier_report.tiers if tier["tier_id"] == "reviewed_fallback"),
                0,
            ),
        },
        temporal_comparison=temporal_comparison,
    )


def build_review_queue_report(
    input_dir: Path,
    runtime: RuntimeConfig | None = None,
) -> ReviewQueueReport:
    gap_report = build_gap_report(input_dir=input_dir, top_n=100000)
    validation_audit_report = build_validation_audit_report(input_dir=input_dir, top_n=100000)

    decisions = _read_if_exists(
        input_dir / "repo_inclusion_decisions.parquet",
        columns=["repo_id", "full_name", "passed_major_filter", "primary_segment"],
    )
    repo_rows = {
        row["repo_id"]: row
        for row in _read_if_exists(
            input_dir / "repos.parquet",
            columns=["repo_id", "full_name", "stars"],
        )
    }
    edges = _read_if_exists(
        input_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "evidence_type", "technology_id"],
    )
    repo_edge_types: dict[int, set[str]] = {}
    repo_technology_ids: dict[int, set[str]] = {}
    for row in edges:
        repo_id = row["repo_id"]
        repo_edge_types.setdefault(repo_id, set()).add(row.get("evidence_type") or "unknown")
        if row.get("technology_id"):
            repo_technology_ids.setdefault(repo_id, set()).add(row["technology_id"])

    readme_only_finals: list[dict[str, Any]] = []
    for row in decisions:
        if not row.get("passed_major_filter"):
            continue
        repo_id = row["repo_id"]
        if classify_repo_evidence_profile(repo_edge_types.get(repo_id, set())) != "readme_only":
            continue
        repo = repo_rows.get(repo_id, {})
        readme_only_finals.append(
            {
                "repo_id": repo_id,
                "full_name": row.get("full_name") or repo.get("full_name"),
                "stars": int(repo.get("stars", 0) or 0),
                "primary_segment": row.get("primary_segment") or "unassigned",
                "evidence_profile": "readme_only",
                "technology_ids": sorted(repo_technology_ids.get(repo_id, set())),
            }
        )
    readme_only_finals = sorted(
        readme_only_finals,
        key=lambda row: (-row["stars"], row["full_name"] or ""),
    )

    benchmark_priority_gaps: list[dict[str, Any]] = []
    benchmark_report_payload = _load_json_if_exists(input_dir / "benchmark_recall_report.json")
    if benchmark_report_payload is None and runtime is not None and runtime.benchmarks.entities:
        benchmark_report_payload = build_benchmark_recall_report(
            input_dir=input_dir,
            runtime=runtime,
        ).__dict__
    if benchmark_report_payload is not None:
        benchmark_entities = {
            row["entity_id"]: row for row in benchmark_report_payload.get("entities", [])
        }
        for row in benchmark_report_payload.get("prioritized_gaps", []):
            entity = benchmark_entities.get(row["entity_id"], {})
            benchmark_priority_gaps.append(
                {
                    **row,
                    "expectation": entity.get("expectation"),
                    "split": entity.get("split"),
                    "segment_id": entity.get("segment_id"),
                }
            )

    audit_changed_repos = (
        validation_audit_report.changed_repos if validation_audit_report is not None else []
    )

    prioritized_review_items: list[dict[str, Any]] = []
    for row in gap_report.final_repos_missing_edges:
        prioritized_review_items.append(
            {
                "review_kind": "missing_edge_final",
                "priority_score": 1000 + int(row.get("unmatched_dependency_count", 0) or 0),
                **row,
            }
        )
    for row in readme_only_finals:
        prioritized_review_items.append(
            {
                "review_kind": "readme_only_final",
                "priority_score": int(row.get("stars", 0) or 0),
                **row,
            }
        )
    for row in audit_changed_repos:
        prioritized_review_items.append(
            {
                "review_kind": "audit_changed_repo",
                "priority_score": 500 + int(row.get("stars", 0) or 0),
                **row,
            }
        )
    for row in benchmark_priority_gaps:
        prioritized_review_items.append(
            {
                "review_kind": "benchmark_priority_gap",
                "priority_score": int(row.get("priority_score", 0) or 0),
                **row,
            }
        )
    prioritized_review_items = sorted(
        prioritized_review_items,
        key=lambda row: (
            -int(row.get("priority_score", 0) or 0),
            row["review_kind"],
            str(row.get("full_name") or row.get("entity_id") or ""),
        ),
    )

    return ReviewQueueReport(
        total_review_item_count=len(prioritized_review_items),
        missing_edge_final_count=gap_report.final_repos_missing_edges_count,
        readme_only_final_count=len(readme_only_finals),
        audit_changed_repo_count=len(audit_changed_repos),
        benchmark_priority_gap_count=len(benchmark_priority_gaps),
        missing_edge_finals=gap_report.final_repos_missing_edges,
        readme_only_finals=readme_only_finals,
        audit_changed_repos=audit_changed_repos,
        benchmark_priority_gaps=benchmark_priority_gaps,
        prioritized_review_items=prioritized_review_items,
    )


def _read_if_exists(path: Path, columns: list[str] | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        return pq.read_table(path, columns=columns).to_pylist()
    except Exception:
        rows = pq.read_table(path).to_pylist()
        if columns is None:
            return rows
        return [{column: row.get(column) for column in columns} for row in rows]


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _load_dependency_evidence_rows(
    *,
    input_dir: Path,
    repo_by_id: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence_path = input_dir / "repo_dependency_evidence.parquet"
    if evidence_path.exists():
        return _read_if_exists(
            evidence_path,
            columns=["repo_id", "package_name", "technology_id", "source_path", "evidence_type"],
        )

    rows: list[dict[str, Any]] = []
    for context in _read_if_exists(input_dir / "repo_contexts.parquet"):
        for dependency in (
            (context.get("manifest_dependencies") or [])
            + (context.get("sbom_dependencies") or [])
            + (context.get("import_dependencies") or [])
        ):
            rows.append(
                {
                    "repo_id": context["repo_id"],
                    "full_name": repo_by_id.get(context["repo_id"], {}).get("full_name"),
                    "package_name": dependency.get("package_name"),
                    "technology_id": dependency.get("technology_id"),
                    "source_path": dependency.get("source_path"),
                    "evidence_type": dependency.get("evidence_type"),
                }
            )
    return rows


def infer_package_prefix(package_name: str) -> str:
    normalized = package_name.casefold()
    if normalized.startswith("@") and "/" in normalized:
        return normalized.split("/", 1)[0] + "/"
    for separator in ("-", "_", "."):
        if separator in normalized:
            return normalized.split(separator, 1)[0]
    return normalized


def is_noisy_unmatched_package(package_name: str) -> bool:
    normalized = package_name.casefold()
    if normalized in NOISY_UNMATCHED_PACKAGE_NAMES:
        return True
    return normalized.startswith(NOISY_UNMATCHED_PACKAGE_PREFIXES)


def is_commodity_unmatched_prefix(package_prefix: str, ai_occurrence_count: int = 0) -> bool:
    normalized = package_prefix.casefold()
    if normalized in COMMODITY_UNMATCHED_PACKAGE_PREFIXES:
        return True
    if normalized.startswith(("@rollup/", "@types/")):
        return True
    return ai_occurrence_count <= 0 and not package_has_ai_affinity(normalized)


def is_ai_specific_unmatched_prefix(package_prefix: str, ai_occurrence_count: int = 0) -> bool:
    normalized = package_prefix.casefold()
    if is_commodity_unmatched_prefix(normalized, ai_occurrence_count=ai_occurrence_count):
        return False
    return ai_occurrence_count > 0 or package_has_ai_affinity(normalized)


def build_vendor_terms(technologies: list[dict[str, Any]]) -> set[str]:
    terms: set[str] = set()
    stopwords = {
        "ai",
        "sdk",
        "agent",
        "agents",
        "browser",
        "use",
        "tool",
        "tools",
        "runtime",
        "open",
        "source",
    }
    for row in technologies:
        for repo_name in row.get("repo_names", []) or []:
            owner, _, repo = repo_name.casefold().partition("/")
            for candidate in (owner, repo):
                for token in split_vendor_tokens(candidate):
                    if len(token) >= 4 and token not in stopwords:
                        terms.add(token)
        for alias in row.get("aliases", []) or []:
            for token in split_vendor_tokens(alias):
                if len(token) >= 4 and token not in stopwords:
                    terms.add(token)
    return terms


def split_vendor_tokens(value: str) -> list[str]:
    cleaned = (
        value.casefold()
        .replace("@", " ")
        .replace("/", " ")
        .replace("-", " ")
        .replace("_", " ")
        .replace(".", " ")
    )
    return [token for token in cleaned.split() if token]


def find_vendor_terms(*, row: dict[str, Any], vendor_terms: set[str]) -> set[str]:
    haystack = " ".join(
        [
            row.get("full_name", ""),
            row.get("description", "") or "",
            " ".join(row.get("topics", []) or []),
        ]
    ).casefold()
    return {term for term in vendor_terms if term in haystack}


def build_benchmark_recall_report(
    *,
    input_dir: Path,
    runtime: RuntimeConfig,
) -> BenchmarkRecallReport:
    repos = _read_if_exists(input_dir / "repos.parquet")
    repo_by_id = {row["repo_id"]: row for row in repos}
    repo_by_name = {row["full_name"].casefold(): row for row in repos}
    decisions = _read_if_exists(input_dir / "repo_inclusion_decisions.parquet")
    final_repo_ids = {row["repo_id"] for row in decisions if row.get("passed_major_filter")}
    edges = _read_if_exists(input_dir / "repo_technology_edges.parquet")
    evidence_rows = _load_dependency_evidence_rows(input_dir=input_dir, repo_by_id=repo_by_id)

    entity_rows: list[dict[str, Any]] = []
    discovered_count = 0
    discovered_by_anchor_count = 0
    discovered_by_search_count = 0
    discovered_by_seed_only_count = 0
    included_count = 0
    identity_count = 0
    adoption_count = 0
    evidence_count = 0
    negative_excluded_count = 0
    negative_discovered_count = 0
    positive_entities = [entity for entity in runtime.benchmarks.entities if entity.expectation == "positive"]
    negative_entities = [entity for entity in runtime.benchmarks.entities if entity.expectation == "negative"]
    holdout_positive_entities = [
        entity for entity in positive_entities if entity.split == "holdout"
    ]
    holdout_discovered_count = 0
    holdout_included_count = 0

    anchor_ids = llm_anchor_technology_ids(runtime)

    for entity in runtime.benchmarks.entities:
        technology_ids = set(entity.technology_ids or [entity.entity_id])
        repo_names = {name.casefold() for name in entity.repo_names}
        package_prefixes = set(
            entity.package_prefixes or _package_prefixes_for_entity(entity, runtime)
        )

        discovered_repos = [repo for name, repo in repo_by_name.items() if name in repo_names]
        discovered_repo_names = sorted(repo["full_name"] for repo in discovered_repos)
        discovered_repo_names_by_search = sorted(
            repo["full_name"] for repo in discovered_repos if repo_discovered_by_search(repo)
        )
        discovered_repo_names_by_anchor = sorted(
            repo["full_name"] for repo in discovered_repos if repo_discovered_by_anchor(repo)
        )
        discovered_repo_names_by_seed_only = sorted(
            repo["full_name"]
            for repo in discovered_repos
            if repo_discovered_by_seed_only(repo)
        )
        included_repo_names = sorted(
            repo_by_id[repo_id]["full_name"]
            for repo_id in final_repo_ids
            if repo_by_id.get(repo_id, {}).get("full_name", "").casefold() in repo_names
        )
        repo_identity_names = sorted(
            {
                edge["full_name"]
                for edge in edges
                if edge.get("technology_id") in technology_ids
                and edge.get("match_method") == "repo_identity"
                and edge.get("full_name", "").casefold() in repo_names
            }
        )
        third_party_adoption_repo_ids = sorted(
            {
                edge["repo_id"]
                for edge in edges
                if edge.get("technology_id") in technology_ids
                and edge.get("full_name", "").casefold() not in repo_names
            }
        )
        dependency_evidence_repo_ids = sorted(
            {
                row["repo_id"]
                for row in evidence_rows
                if row.get("repo_id") is not None
                and (
                    row.get("technology_id") in technology_ids
                    or package_matches_prefixes(row.get("package_name"), package_prefixes)
                )
            }
        )
        third_party_dependency_evidence_repo_ids = sorted(
            {
                repo_id
                for repo_id in dependency_evidence_repo_ids
                if repo_by_id.get(repo_id, {}).get("full_name", "").casefold() not in repo_names
            }
        )

        entity_row = {
            "entity_id": entity.entity_id,
            "display_name": entity.display_name,
            "expectation": entity.expectation,
            "split": entity.split,
            "segment_id": entity.segment_id,
            "repo_discovered": bool(discovered_repo_names),
            "repo_discovered_by_anchor": bool(discovered_repo_names_by_anchor),
            "repo_discovered_by_search": bool(discovered_repo_names_by_search),
            "repo_discovered_by_seed_only": bool(discovered_repo_names_by_seed_only),
            "repo_included": bool(included_repo_names),
            "repo_identity_mapped": bool(repo_identity_names),
            "third_party_adoption": bool(third_party_adoption_repo_ids),
            "dependency_evidence_found": bool(dependency_evidence_repo_ids),
            "third_party_dependency_evidence_found": bool(third_party_dependency_evidence_repo_ids),
            "negative_repo_excluded": not bool(included_repo_names),
            "discovered_repo_names": discovered_repo_names,
            "discovered_repo_names_by_anchor": discovered_repo_names_by_anchor,
            "discovered_repo_names_by_search": discovered_repo_names_by_search,
            "discovered_repo_names_by_seed_only": discovered_repo_names_by_seed_only,
            "included_repo_names": included_repo_names,
            "repo_identity_mapped_names": repo_identity_names,
            "third_party_adoption_repo_count": len(third_party_adoption_repo_ids),
            "dependency_evidence_repo_count": len(dependency_evidence_repo_ids),
            "third_party_dependency_evidence_repo_count": len(
                third_party_dependency_evidence_repo_ids
            ),
            "is_anchor_entity": bool(technology_ids & anchor_ids),
        }
        entity_rows.append(entity_row)
        if entity.expectation == "positive":
            discovered_count += int(entity_row["repo_discovered"])
            discovered_by_anchor_count += int(entity_row["repo_discovered_by_anchor"])
            discovered_by_search_count += int(entity_row["repo_discovered_by_search"])
            discovered_by_seed_only_count += int(entity_row["repo_discovered_by_seed_only"])
            included_count += int(entity_row["repo_included"])
            identity_count += int(entity_row["repo_identity_mapped"])
            adoption_count += int(entity_row["third_party_adoption"])
            evidence_count += int(entity_row["dependency_evidence_found"])
            if entity.split == "holdout":
                holdout_discovered_count += int(entity_row["repo_discovered"])
                holdout_included_count += int(entity_row["repo_included"])
        else:
            negative_excluded_count += int(entity_row["negative_repo_excluded"])
            negative_discovered_count += int(entity_row["repo_discovered"])

    entity_count = len(positive_entities)
    total_entity_count = len(entity_rows)
    negative_entity_count = len(negative_entities)
    holdout_entity_count = len(holdout_positive_entities)
    tuning_entity_count = len(positive_entities) - holdout_entity_count
    thresholds = runtime.benchmarks.thresholds
    rates = {
        "repo_discovered_rate": ratio(discovered_count, entity_count),
        "repo_discovered_by_anchor_rate": ratio(discovered_by_anchor_count, entity_count),
        "repo_discovered_by_search_rate": ratio(discovered_by_search_count, entity_count),
        "repo_discovered_by_seed_only_rate": ratio(discovered_by_seed_only_count, entity_count),
        "repo_included_rate": ratio(included_count, entity_count),
        "repo_identity_mapped_rate": ratio(identity_count, entity_count),
        "third_party_adoption_rate": ratio(adoption_count, entity_count),
        "dependency_evidence_rate": ratio(evidence_count, entity_count),
        "negative_repo_excluded_rate": ratio(negative_excluded_count, negative_entity_count),
        "negative_repo_discovered_rate": ratio(negative_discovered_count, negative_entity_count),
        "holdout_repo_discovered_rate": ratio(holdout_discovered_count, holdout_entity_count),
        "holdout_repo_included_rate": ratio(holdout_included_count, holdout_entity_count),
    }
    threshold_rows = [
        {
            "metric": "repo_discovered_rate",
            "actual": rates["repo_discovered_rate"],
            "minimum": thresholds.min_repo_discovered_rate,
            "severity": thresholds.severity,
        },
        {
            "metric": "repo_included_rate",
            "actual": rates["repo_included_rate"],
            "minimum": thresholds.min_repo_included_rate,
            "severity": thresholds.severity,
        },
        {
            "metric": "repo_identity_mapped_rate",
            "actual": rates["repo_identity_mapped_rate"],
            "minimum": thresholds.min_repo_identity_mapped_rate,
            "severity": thresholds.severity,
        },
        {
            "metric": "third_party_adoption_rate",
            "actual": rates["third_party_adoption_rate"],
            "minimum": thresholds.min_third_party_adoption_rate,
            "severity": thresholds.severity,
        },
        {
            "metric": "dependency_evidence_rate",
            "actual": rates["dependency_evidence_rate"],
            "minimum": thresholds.min_dependency_evidence_rate,
            "severity": thresholds.severity,
        },
    ]
    if negative_entity_count:
        threshold_rows.append(
            {
                "metric": "negative_repo_excluded_rate",
                "actual": rates["negative_repo_excluded_rate"],
                "minimum": thresholds.min_negative_repo_excluded_rate,
                "severity": thresholds.severity,
            }
        )
    if holdout_entity_count:
        threshold_rows.extend(
            [
                {
                    "metric": "holdout_repo_discovered_rate",
                    "actual": rates["holdout_repo_discovered_rate"],
                    "minimum": thresholds.min_holdout_repo_discovered_rate,
                    "severity": thresholds.severity,
                },
                {
                    "metric": "holdout_repo_included_rate",
                    "actual": rates["holdout_repo_included_rate"],
                    "minimum": thresholds.min_holdout_repo_included_rate,
                    "severity": thresholds.severity,
                },
            ]
        )
    failed_thresholds = [
        row for row in threshold_rows if row["actual"] + 1e-9 < row["minimum"]
    ]

    return BenchmarkRecallReport(
        total_entity_count=total_entity_count,
        entity_count=entity_count,
        negative_entity_count=negative_entity_count,
        tuning_entity_count=tuning_entity_count,
        holdout_entity_count=holdout_entity_count,
        entities_with_repo_discovered=discovered_count,
        entities_with_repo_discovered_by_anchor=discovered_by_anchor_count,
        entities_with_repo_discovered_by_search=discovered_by_search_count,
        entities_with_repo_discovered_by_seed_only=discovered_by_seed_only_count,
        entities_with_repo_included=included_count,
        entities_with_repo_identity_mapped=identity_count,
        entities_with_third_party_adoption=adoption_count,
        entities_with_dependency_evidence=evidence_count,
        repo_discovered_rate=rates["repo_discovered_rate"],
        repo_discovered_by_anchor_rate=rates["repo_discovered_by_anchor_rate"],
        repo_discovered_by_search_rate=rates["repo_discovered_by_search_rate"],
        repo_discovered_by_seed_only_rate=rates["repo_discovered_by_seed_only_rate"],
        repo_included_rate=rates["repo_included_rate"],
        repo_identity_mapped_rate=rates["repo_identity_mapped_rate"],
        third_party_adoption_rate=rates["third_party_adoption_rate"],
        dependency_evidence_rate=rates["dependency_evidence_rate"],
        negative_repo_excluded_rate=rates["negative_repo_excluded_rate"],
        negative_repo_discovered_rate=rates["negative_repo_discovered_rate"],
        holdout_repo_discovered_rate=rates["holdout_repo_discovered_rate"],
        holdout_repo_included_rate=rates["holdout_repo_included_rate"],
        thresholds={
            "min_repo_discovered_rate": thresholds.min_repo_discovered_rate,
            "min_repo_included_rate": thresholds.min_repo_included_rate,
            "min_repo_identity_mapped_rate": thresholds.min_repo_identity_mapped_rate,
            "min_third_party_adoption_rate": thresholds.min_third_party_adoption_rate,
            "min_dependency_evidence_rate": thresholds.min_dependency_evidence_rate,
            "min_negative_repo_excluded_rate": thresholds.min_negative_repo_excluded_rate,
            "min_holdout_repo_discovered_rate": thresholds.min_holdout_repo_discovered_rate,
            "min_holdout_repo_included_rate": thresholds.min_holdout_repo_included_rate,
            "severity": thresholds.severity,
        },
        failed_thresholds=failed_thresholds,
        prioritized_gaps=prioritize_benchmark_gaps([row for row in entity_rows if row["expectation"] == "positive"]),
        entities=entity_rows,
    )


def _package_prefixes_for_entity(entity, runtime: RuntimeConfig) -> list[str]:
    prefixes: list[str] = []
    for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]:
        if (
            technology.technology_id == entity.entity_id
            or technology.technology_id in entity.technology_ids
        ):
            prefixes.extend(technology.package_prefixes)
    return prefixes


def package_matches_prefixes(package_name: str | None, prefixes: set[str]) -> bool:
    if not package_name or not prefixes:
        return False
    normalized_package = package_name.casefold()
    for prefix in prefixes:
        normalized_prefix = prefix.casefold()
        if normalized_prefix.endswith(("/", "-", "_", ".")):
            if normalized_package.startswith(normalized_prefix):
                return True
            continue
        if normalized_package == normalized_prefix:
            return True
        if (
            normalized_package.startswith(f"{normalized_prefix}-")
            or normalized_package.startswith(f"{normalized_prefix}_")
            or normalized_package.startswith(f"{normalized_prefix}/")
            or normalized_package.startswith(f"{normalized_prefix}.")
        ):
            return True
    return False


def repo_discovered_by_search(repo: dict[str, Any]) -> bool:
    source_types = set(repo.get("discovery_source_types") or [])
    if {"topic_query", "description_query"} & source_types:
        return True

    queries = repo.get("discovery_queries") or []
    return any(not str(query).startswith("repo:") for query in queries)


def repo_discovered_by_anchor(repo: dict[str, Any]) -> bool:
    source_types = set(repo.get("discovery_source_types") or [])
    return "anchor_seed" in source_types


def repo_discovered_by_seed_only(repo: dict[str, Any]) -> bool:
    queries = repo.get("discovery_queries") or []
    if not queries:
        return False
    return not repo_discovered_by_search(repo) and any(
        str(query).startswith("repo:") for query in queries
    )


def ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def wilson_interval(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return (0.0, 0.0)
    phat = successes / total
    denom = 1 + (z * z) / total
    center = (phat + (z * z) / (2 * total)) / denom
    margin = (
        z
        * math.sqrt((phat * (1 - phat) + (z * z) / (4 * total)) / total)
        / denom
    )
    return (max(0.0, center - margin), min(1.0, center + margin))


def classify_repo_evidence_profile(evidence_types: set[str]) -> str:
    if not evidence_types:
        return "unmapped"
    if evidence_types == {"readme_mention"}:
        return "readme_only"
    if evidence_types == {"repo_identity"}:
        return "repo_identity_only"
    if evidence_types <= {"repo_identity", "readme_mention"}:
        return "repo_identity_plus_fallback"
    if "readme_mention" in evidence_types:
        return "mixed_direct_and_fallback"
    if evidence_types & {"manifest", "sbom", "import"}:
        return "direct_only"
    return "other"


def score_band(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 1:
        return "0-1"
    if value <= 3:
        return "2-3"
    if value <= 5:
        return "4-5"
    if value <= 8:
        return "6-8"
    return "9+"


def load_temporal_baseline_dir(input_dir: Path) -> Path | None:
    validation_summary_path = input_dir / "validation_sample_summary.json"
    if validation_summary_path.exists():
        payload = _load_json_if_exists(validation_summary_path) or {}
        baseline = payload.get("input_dir")
        if baseline:
            return Path(str(baseline)).resolve()
    repair_summary_path = input_dir / "repair_summary.json"
    if repair_summary_path.exists():
        payload = _load_json_if_exists(repair_summary_path) or {}
        baseline = payload.get("input_dir")
        if baseline:
            return Path(str(baseline)).resolve()
    return None


def prioritize_benchmark_gaps(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prioritized: list[dict[str, Any]] = []
    for entity in entities:
        reasons: list[str] = []
        suggested_actions: list[str] = []
        score = 0
        if not entity["repo_discovered"]:
            score += 100
            reasons.append("benchmark repo not discovered")
            suggested_actions.append("add discovery seed or discovery query coverage")
        if entity["repo_discovered"] and not entity.get("repo_discovered_by_search"):
            score += 30
            reasons.append("benchmark repo discovered only via exact seed")
            suggested_actions.append("improve topic or description query coverage")
        if entity["repo_discovered"] and entity.get("is_anchor_entity") and not entity.get(
            "repo_discovered_by_anchor"
        ):
            score += 20
            reasons.append("anchor entity is not covered by anchor seed inputs")
            suggested_actions.append("add or fix anchor repo_names coverage")
        if entity["repo_discovered"] and not entity["repo_included"]:
            score += 70
            reasons.append("benchmark repo discovered but not included")
            suggested_actions.append("review classification thresholds or exclusions")
        if entity["repo_included"] and not entity["repo_identity_mapped"]:
            score += 80
            reasons.append("repo included but canonical repo identity is not mapped")
            suggested_actions.append("add or fix repo_names mapping in registry")
        if (
            entity.get("third_party_dependency_evidence_found")
            and not entity["third_party_adoption"]
        ):
            score += 60
            reasons.append(
                "dependency evidence exists but no third-party adoption edge was created"
            )
            suggested_actions.append("add or fix package_prefix mapping in registry")
        if not entity["dependency_evidence_found"]:
            score += 40
            reasons.append("no dependency evidence found for benchmark entity")
            suggested_actions.append("expand package prefixes or manifest/SBOM extraction coverage")
        if score <= 0:
            continue
        prioritized.append(
            {
                "entity_id": entity["entity_id"],
                "display_name": entity["display_name"],
                "priority_score": score,
                "reasons": reasons,
                "suggested_actions": unique_ordered(suggested_actions),
            }
        )
    return sorted(prioritized, key=lambda row: (-row["priority_score"], row["entity_id"]))


def build_suggested_discovery_inputs(
    *,
    ai_specific_prefixes: Counter[str],
    vendor_like_unmapped: list[dict[str, Any]],
    top_n: int,
) -> list[dict[str, Any]]:
    package_entries = [
        {
            "entry_type": "package_prefix",
            "value": prefix,
            "priority_score": count * 10,
            "evidence_count": count,
            "suggested_action": (
                "promote to registry prefix or canonical upstream seed if identity is known"
            ),
        }
        for prefix, count in ai_specific_prefixes.most_common(top_n)
    ]
    repo_entries = [
        {
            "entry_type": "repo_seed",
            "value": row["full_name"],
            "priority_score": int(row.get("stars", 0) or 0),
            "stars": int(row.get("stars", 0) or 0),
            "matched_terms": row.get("matched_terms", []),
            "suggested_action": "consider exact repo seed and canonical repo_names mapping",
        }
        for row in vendor_like_unmapped[:top_n]
    ]
    combined = sorted(
        [*package_entries, *repo_entries],
        key=lambda row: (-row["priority_score"], row["entry_type"], row["value"]),
    )
    return combined[:top_n]


def unique_ordered(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
