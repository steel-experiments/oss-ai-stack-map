from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


class DiscoveredRepo(BaseModel):
    repo_id: int
    full_name: str
    html_url: str
    description: str | None = None
    owner_type: str | None = None
    stars: int
    forks: int
    primary_language: str | None = None
    topics: list[str] = Field(default_factory=list)
    license_spdx: str | None = None
    is_archived: bool
    is_fork: bool
    is_template: bool
    created_at: str
    updated_at: str
    pushed_at: str
    snapshot_date: date
    discovery_queries: list[str] = Field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class DiscoveryResult(BaseModel):
    repositories: list[DiscoveredRepo]
    queries: list[str]
    stage_timings: list["StageTiming"] = Field(default_factory=list)


class ManifestDependency(BaseModel):
    package_name: str
    dependency_scope: str = "runtime"
    source_path: str
    evidence_type: str = "manifest"
    confidence: str = "high"
    raw_specifier: str | None = None
    raw_version: str | None = None
    purl: str | None = None
    license_spdx: str | None = None
    technology_id: str | None = None
    provider_id: str | None = None

    def to_row(self, repo_id: int, snapshot_date: date) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["repo_id"] = repo_id
        payload["snapshot_date"] = snapshot_date.isoformat()
        return payload


class RepoContext(BaseModel):
    repo_id: int
    full_name: str
    default_branch: str = "HEAD"
    readme_text: str = ""
    tree_paths: list[str] = Field(default_factory=list)
    manifest_paths: list[str] = Field(default_factory=list)
    manifest_dependencies: list[ManifestDependency] = Field(default_factory=list)
    sbom_dependencies: list[ManifestDependency] = Field(default_factory=list)
    import_dependencies: list[ManifestDependency] = Field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ClassificationDecision(BaseModel):
    repo_id: int
    full_name: str
    passed_candidate_filter: bool
    passed_serious_filter: bool
    passed_ai_relevance_filter: bool
    passed_major_filter: bool
    rule_passed_serious_filter: bool | None = None
    rule_passed_ai_relevance_filter: bool | None = None
    rule_passed_major_filter: bool | None = None
    score_serious: int
    score_ai: int
    exclusion_reason: str | None = None
    primary_segment: str | None = None
    segments: list[str] = Field(default_factory=list)
    judge_applied: bool = False
    judge_override_applied: bool = False
    judge_mode: Literal["hardening", "validation"] | None = None
    judge_confidence: str | None = None
    judge_include_in_final_set: bool | None = None
    judge_serious_project: bool | None = None
    judge_ai_relevant: bool | None = None
    judge_primary_segment: str | None = None
    judge_reasons: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ClassificationSummary(BaseModel):
    total: int
    passed_serious: int
    passed_ai: int
    passed_major: int
    stage_timings: list["StageTiming"] = Field(default_factory=list)


class StageTiming(BaseModel):
    stage_id: str
    seconds: float
    item_count: int | None = None
    notes: str | None = None
    attempt_id: str | None = None

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class RepoTechnologyEdge(BaseModel):
    repo_id: int
    full_name: str
    technology_id: str
    category_id: str | None = None
    raw_signal: str
    raw_version: str | None = None
    evidence_type: str
    evidence_path: str
    direct: bool = True
    dependency_scope: str = "runtime"
    confidence: str = "high"
    provider_id: str | None = None
    purl: str | None = None
    license_spdx: str | None = None
    snapshot_date: date

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class JudgeDecision(BaseModel):
    repo_id: int
    full_name: str
    judge_mode: Literal["hardening", "validation"] = "hardening"
    serious_project: bool
    ai_relevant: bool
    include_in_final_set: bool
    primary_segment: str | None = None
    confidence: str
    override_rule_decision: bool
    reasons: list[str] = Field(default_factory=list)
    model: str
    applied: bool = False

    def to_row(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
