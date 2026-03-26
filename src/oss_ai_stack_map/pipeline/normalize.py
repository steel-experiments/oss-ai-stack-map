from __future__ import annotations

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    ManifestDependency,
    RepoContext,
    RepoTechnologyEdge,
)


def build_repo_technology_edges(
    runtime: RuntimeConfig,
    contexts: list[RepoContext],
    decisions: list[ClassificationDecision],
) -> list[RepoTechnologyEdge]:
    decision_by_repo = {decision.repo_id: decision for decision in decisions}
    technologies = {
        technology.technology_id: technology for technology in runtime.aliases.technologies
    }
    edges: list[RepoTechnologyEdge] = []

    for context in contexts:
        decision = decision_by_repo.get(context.repo_id)
        if not decision or not decision.passed_major_filter:
            continue

        by_technology: dict[str, ManifestDependency] = {}
        for evidence in (
            context.manifest_dependencies
            + context.sbom_dependencies
            + context.import_dependencies
        ):
            if not evidence.technology_id:
                continue
            existing = by_technology.get(evidence.technology_id)
            if existing is None or evidence_rank(evidence) > evidence_rank(existing):
                by_technology[evidence.technology_id] = evidence

        for technology_id, evidence in sorted(by_technology.items()):
            technology = technologies.get(technology_id)
            edges.append(
                RepoTechnologyEdge(
                    repo_id=context.repo_id,
                    full_name=context.full_name,
                    technology_id=technology_id,
                    category_id=technology.category_id if technology else None,
                    raw_signal=evidence.package_name,
                    raw_version=evidence.raw_version or evidence.raw_specifier,
                    evidence_type=evidence.evidence_type,
                    evidence_path=evidence.source_path,
                    direct=True,
                    dependency_scope=evidence.dependency_scope,
                    confidence=evidence.confidence,
                    provider_id=evidence.provider_id,
                    purl=evidence.purl,
                    license_spdx=evidence.license_spdx,
                    snapshot_date=runtime.study.snapshot_date,
                )
            )

    return edges


def build_technology_rows(runtime: RuntimeConfig) -> list[dict]:
    rows = []
    for technology in runtime.aliases.technologies:
        rows.append(
            {
                "technology_id": technology.technology_id,
                "display_name": technology.display_name,
                "category_id": technology.category_id,
                "provider_id": technology.provider_id,
                "aliases": technology.aliases,
            }
        )
    return rows


def evidence_rank(evidence: ManifestDependency) -> tuple[int, int, int]:
    evidence_priority = {"manifest": 3, "sbom": 2, "import": 1}
    scope_priority = {"runtime": 3, "optional": 2, "prod": 2, "unknown": 1, "dev": 0}
    confidence_priority = {"high": 3, "medium": 2, "low": 1}
    return (
        evidence_priority.get(evidence.evidence_type, 0),
        confidence_priority.get(evidence.confidence, 0),
        scope_priority.get(evidence.dependency_scope, 0),
    )
