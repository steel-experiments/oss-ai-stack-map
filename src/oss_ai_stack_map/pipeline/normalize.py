from __future__ import annotations

import re

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
    technologies = {}
    repo_lookup = {}
    for technology in runtime.aliases.technologies:
        technologies[technology.technology_id] = technology
        for repo_name in technology.repo_names:
            repo_lookup[repo_name.casefold()] = technology
    for technology in runtime.registry.technologies:
        technologies[technology.technology_id] = technology
        for repo_name in technology.repo_names:
            repo_lookup[repo_name.casefold()] = technology
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
            provider_technology_id = evidence.provider_technology_id
            if provider_technology_id and provider_technology_id != evidence.technology_id:
                provider_evidence = build_provider_subedge_evidence(
                    evidence,
                    provider_technology_id=provider_technology_id,
                )
                existing_provider = by_technology.get(provider_technology_id)
                if existing_provider is None or evidence_rank(provider_evidence) > evidence_rank(
                    existing_provider
                ):
                    by_technology[provider_technology_id] = provider_evidence

        repo_identity = repo_lookup.get(context.full_name.casefold())
        if repo_identity is not None:
            identity_evidence = build_repo_identity_evidence(context.full_name, repo_identity)
            existing_identity = by_technology.get(repo_identity.technology_id)
            if existing_identity is None or evidence_rank(identity_evidence) > evidence_rank(
                existing_identity
            ):
                by_technology[repo_identity.technology_id] = identity_evidence

        if runtime.study.classification.readme_mentions_used_for_edges and not by_technology:
            for evidence in build_readme_alias_evidence(runtime=runtime, context=context):
                existing = by_technology.get(evidence.technology_id or "")
                if evidence.technology_id and (
                    existing is None or evidence_rank(evidence) > evidence_rank(existing)
                ):
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
                    entity_type=evidence.entity_type,
                    canonical_product_id=evidence.canonical_product_id,
                    match_method=evidence.match_method,
                    evidence_strength=evidence_strength(evidence.evidence_type),
                    purl=evidence.purl,
                    license_spdx=evidence.license_spdx,
                    snapshot_date=runtime.study.snapshot_date,
                )
            )

    return edges


def build_technology_rows(runtime: RuntimeConfig) -> list[dict]:
    rows_by_id: dict[str, dict] = {}
    for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]:
        rows_by_id[technology.technology_id] = {
            "technology_id": technology.technology_id,
            "display_name": technology.display_name,
            "category_id": technology.category_id,
            "provider_id": technology.provider_id,
            "entity_type": technology.entity_type,
            "canonical_product_id": technology.canonical_product_id,
            "aliases": technology.aliases,
            "package_prefixes": technology.package_prefixes,
            "repo_names": technology.repo_names,
            "capabilities": technology.capabilities,
        }
    return list(rows_by_id.values())


def evidence_rank(evidence: ManifestDependency) -> tuple[int, int, int]:
    evidence_priority = {
        "repo_identity": 4,
        "manifest": 3,
        "sbom": 2,
        "import": 1,
        "readme_mention": 0,
    }
    scope_priority = {"runtime": 3, "optional": 2, "prod": 2, "unknown": 1, "dev": 0}
    confidence_priority = {"high": 3, "medium": 2, "low": 1}
    return (
        evidence_priority.get(evidence.evidence_type, 0),
        confidence_priority.get(evidence.confidence, 0),
        scope_priority.get(evidence.dependency_scope, 0),
    )


def evidence_strength(evidence_type: str) -> str:
    strength_by_type = {
        "repo_identity": "high",
        "manifest": "high",
        "sbom": "high",
        "import": "medium",
        "readme_mention": "low",
    }
    return strength_by_type.get(evidence_type, "low")


def build_provider_subedge_evidence(
    evidence: ManifestDependency,
    *,
    provider_technology_id: str,
) -> ManifestDependency:
    return ManifestDependency(
        package_name=evidence.package_name,
        dependency_scope=evidence.dependency_scope,
        source_path=evidence.source_path,
        evidence_type=evidence.evidence_type,
        confidence=evidence.confidence,
        raw_specifier=evidence.raw_specifier,
        raw_version=evidence.raw_version,
        purl=evidence.purl,
        license_spdx=evidence.license_spdx,
        technology_id=provider_technology_id,
        provider_id=evidence.provider_id,
        provider_technology_id=provider_technology_id,
        entity_type="provider",
        canonical_product_id=evidence.canonical_product_id,
        match_method="derived_provider",
    )


def build_repo_identity_evidence(full_name: str, technology) -> ManifestDependency:
    return ManifestDependency(
        package_name=full_name,
        dependency_scope="runtime",
        source_path="repo_metadata",
        evidence_type="repo_identity",
        confidence="high",
        raw_specifier=full_name,
        technology_id=technology.technology_id,
        provider_id=technology.provider_id,
        provider_technology_id=technology.technology_id,
        entity_type=technology.entity_type,
        canonical_product_id=technology.canonical_product_id,
        match_method="repo_identity",
    )


def build_readme_alias_evidence(
    *,
    runtime: RuntimeConfig,
    context: RepoContext,
) -> list[ManifestDependency]:
    if not context.readme_text:
        return []
    readme_text = context.readme_text.casefold()[:50000]
    evidences: list[ManifestDependency] = []
    seen_technology_ids: set[str] = set()
    for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]:
        if technology.technology_id in seen_technology_ids:
            continue
        for alias in technology.aliases:
            normalized_alias = alias.casefold().strip()
            if not should_use_readme_alias(normalized_alias):
                continue
            if readme_mentions_alias(readme_text, normalized_alias):
                evidences.append(
                    ManifestDependency(
                        package_name=alias,
                        dependency_scope="runtime",
                        source_path="README",
                        evidence_type="readme_mention",
                        confidence="low",
                        technology_id=technology.technology_id,
                        provider_id=technology.provider_id,
                        provider_technology_id=technology.technology_id,
                        entity_type=technology.entity_type,
                        canonical_product_id=technology.canonical_product_id,
                        match_method="readme_alias",
                    )
                )
                seen_technology_ids.add(technology.technology_id)
                break
    return evidences


def should_use_readme_alias(alias: str) -> bool:
    if len(alias) < 4:
        return False
    stopwords = {
        "agent",
        "agents",
        "ai",
        "browser",
        "core",
        "model",
        "models",
        "open",
        "platform",
        "prompt",
        "runtime",
        "sdk",
        "tool",
        "tools",
        "use",
        "workflow",
    }
    return alias not in stopwords


def readme_mentions_alias(readme_text: str, alias: str) -> bool:
    pattern = rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])"
    return re.search(pattern, readme_text) is not None
