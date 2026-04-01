from __future__ import annotations

import math
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FilterConfig(BaseModel):
    candidate_stars_min: int = 1000
    major_stars_min: int = 1000
    freshness_months: int = 1
    max_search_pages_per_query: int = 3
    max_repos: int | None = None


class ClassificationConfig(BaseModel):
    serious_pass_score: int = 3
    ai_relevance_pass_score: int = 3
    strong_serious_override_score: int = 5
    readme_mentions_used_for_edges: bool = False


class OutputConfig(BaseModel):
    write_parquet: bool = True
    write_csv: bool = True
    publish_major_view: bool = True
    publish_provider_rollups: bool = True


class HttpConfig(BaseModel):
    timeout_seconds: float = 30.0
    max_retries: int = 5
    user_agent: str = "oss-ai-stack-map/0.1.0"


class JudgeConfig(BaseModel):
    enabled: bool = False
    hardening_enabled: bool | None = None
    validation_enabled: bool | None = None
    model: str = "gpt-5.4-nano"
    max_cases_per_run: int = 25
    validation_sample_fraction: float | None = None
    validation_sample_seed: int = 0
    override_on_high_confidence: bool = True
    min_confidence_to_override: str = "high"
    reasoning_effort: str = "low"

    @staticmethod
    def _normalize_fraction(value: float | None) -> float | None:
        if value is None:
            return None
        return min(max(float(value), 0.0), 1.0)

    def validation_target_case_count(
        self,
        *,
        final_repo_count: int,
        remaining_capacity: int,
    ) -> int:
        if remaining_capacity <= 0 or final_repo_count <= 0:
            return 0
        fraction = self._normalize_fraction(self.validation_sample_fraction)
        if fraction is None:
            return min(final_repo_count, remaining_capacity)
        if fraction <= 0:
            return 0
        target = max(1, math.ceil(final_repo_count * fraction))
        return min(target, final_repo_count, remaining_capacity)

    def mode_enabled(self, judge_mode: Literal["hardening", "validation"]) -> bool:
        if judge_mode == "hardening":
            if self.hardening_enabled is not None:
                return self.hardening_enabled
            return self.enabled
        if self.validation_enabled is not None:
            return self.validation_enabled
        return False

    def any_mode_enabled(self) -> bool:
        return self.mode_enabled("hardening") or self.mode_enabled("validation")


class StudyConfig(BaseModel):
    snapshot_date: date = Field(default_factory=lambda: date.today())
    filters: FilterConfig = Field(default_factory=FilterConfig)
    classification: ClassificationConfig = Field(default_factory=ClassificationConfig)
    outputs: OutputConfig = Field(default_factory=OutputConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    judge: JudgeConfig = Field(default_factory=JudgeConfig)
    checkpoint_batch_size: int = 25
    initial_ecosystems: list[str] = Field(
        default_factory=lambda: ["python", "javascript", "typescript", "go", "rust"]
    )


class DiscoveryConfig(BaseModel):
    topics: list[str]
    description_keywords: list[str]
    manual_seed_repos: list[str]


class ExclusionConfig(BaseModel):
    hard_keywords: list[str]
    educational_keywords: list[str] = Field(default_factory=list)
    excluded_directories: list[str]
    source_extensions: list[str]
    manifest_files: list[str]


class TechnologyAlias(BaseModel):
    technology_id: str
    display_name: str
    category_id: str
    provider_id: str | None = None
    entity_type: str = "technology"
    canonical_product_id: str | None = None
    aliases: list[str]
    import_aliases: list[str] = Field(default_factory=list)
    package_prefixes: list[str] = Field(default_factory=list)
    repo_names: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)


class TechnologyAliasConfig(BaseModel):
    technologies: list[TechnologyAlias] = Field(default_factory=list)

    def alias_lookup(self) -> dict[str, TechnologyAlias]:
        lookup: dict[str, TechnologyAlias] = {}
        for tech in self.technologies:
            for alias in tech.aliases:
                for key in normalized_alias_keys(alias):
                    lookup[key] = tech
        return lookup

    def import_lookup(self) -> dict[str, TechnologyAlias]:
        lookup: dict[str, TechnologyAlias] = {}
        for tech in self.technologies:
            for alias in [*tech.aliases, *tech.import_aliases]:
                for key in normalized_alias_keys(alias):
                    lookup[key] = tech
        return lookup

    def package_prefix_rules(self) -> list[tuple[str, TechnologyAlias]]:
        rules: list[tuple[str, TechnologyAlias]] = []
        for tech in self.technologies:
            for prefix in tech.package_prefixes:
                normalized = prefix.casefold().strip()
                if normalized:
                    rules.append((normalized, tech))
        return sorted(rules, key=lambda item: (-len(item[0]), item[0]))

    def repo_lookup(self) -> dict[str, TechnologyAlias]:
        lookup: dict[str, TechnologyAlias] = {}
        for tech in self.technologies:
            for repo_name in tech.repo_names:
                lookup[repo_name.casefold()] = tech
        return lookup


class SegmentRule(BaseModel):
    segment_id: str
    topic_keywords: list[str] = Field(default_factory=list)
    description_keywords: list[str] = Field(default_factory=list)
    technology_ids: list[str] = Field(default_factory=list)
    config_keywords: list[str] = Field(default_factory=list)


class SegmentConfig(BaseModel):
    precedence: list[str]
    rules: list[SegmentRule]


class BenchmarkEntity(BaseModel):
    entity_id: str
    display_name: str
    technology_ids: list[str] = Field(default_factory=list)
    repo_names: list[str] = Field(default_factory=list)
    package_prefixes: list[str] = Field(default_factory=list)
    expectation: Literal["positive", "negative"] = "positive"
    split: Literal["tuning", "holdout"] = "tuning"
    segment_id: str | None = None
    notes: str | None = None


class BenchmarkThresholds(BaseModel):
    min_repo_discovered_rate: float = 1.0
    min_repo_included_rate: float = 0.5
    min_repo_identity_mapped_rate: float = 0.5
    min_third_party_adoption_rate: float = 0.5
    min_dependency_evidence_rate: float = 0.75
    min_negative_repo_excluded_rate: float = 0.9
    min_holdout_repo_discovered_rate: float = 0.75
    min_holdout_repo_included_rate: float = 0.4
    severity: Literal["warning", "error"] = "warning"


class BenchmarkConfig(BaseModel):
    entities: list[BenchmarkEntity] = Field(default_factory=list)
    thresholds: BenchmarkThresholds = Field(default_factory=BenchmarkThresholds)


class EntityRecord(BaseModel):
    entity_id: str
    display_name: str
    entity_type: str = "company"
    canonical_domains: list[str] = Field(default_factory=list)
    github_orgs: list[str] = Field(default_factory=list)
    repo_names: list[str] = Field(default_factory=list)
    technology_ids: list[str] = Field(default_factory=list)
    notes: str | None = None


class EntityConfig(BaseModel):
    entities: list[EntityRecord] = Field(default_factory=list)

    def repo_lookup(self) -> dict[str, EntityRecord]:
        lookup: dict[str, EntityRecord] = {}
        for entity in self.entities:
            for repo_name in entity.repo_names:
                lookup[repo_name.casefold()] = entity
        return lookup

    def github_org_lookup(self) -> dict[str, EntityRecord]:
        lookup: dict[str, EntityRecord] = {}
        for entity in self.entities:
            for github_org in entity.github_orgs:
                lookup[github_org.casefold()] = entity
        return lookup

    def technology_lookup(self) -> dict[str, EntityRecord]:
        lookup: dict[str, EntityRecord] = {}
        for entity in self.entities:
            for technology_id in entity.technology_ids:
                lookup[technology_id] = entity
        return lookup


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    github_token: str | None = None
    openai_api_key: str | None = None


class RuntimeConfig(BaseModel):
    config_dir: Path
    study: StudyConfig
    discovery: DiscoveryConfig
    exclusions: ExclusionConfig
    aliases: TechnologyAliasConfig
    registry: TechnologyAliasConfig = Field(default_factory=TechnologyAliasConfig)
    benchmarks: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    entities: EntityConfig = Field(default_factory=EntityConfig)
    segments: SegmentConfig
    env: EnvSettings


class ConfigValidationError(ValueError):
    pass


def load_runtime(config_dir: Path = Path("config")) -> RuntimeConfig:
    config_dir = config_dir.resolve()
    study = StudyConfig.model_validate(_load_yaml(config_dir / "study_config.yaml"))
    discovery = DiscoveryConfig.model_validate(_load_yaml(config_dir / "discovery_topics.yaml"))
    exclusions = ExclusionConfig.model_validate(_load_yaml(config_dir / "exclusion_rules.yaml"))
    aliases = TechnologyAliasConfig.model_validate(
        _load_yaml(config_dir / "technology_aliases.yaml")
    )
    registry_path = config_dir / "technology_registry.yaml"
    registry = TechnologyAliasConfig.model_validate(
        _load_yaml(registry_path) if registry_path.exists() else {}
    )
    benchmarks_path = config_dir / "benchmark_entities.yaml"
    benchmarks = BenchmarkConfig.model_validate(
        _load_yaml(benchmarks_path) if benchmarks_path.exists() else {}
    )
    entities_path = config_dir / "entities.yaml"
    entities = EntityConfig.model_validate(
        _load_yaml(entities_path) if entities_path.exists() else {}
    )
    segments = SegmentConfig.model_validate(_load_yaml(config_dir / "segment_rules.yaml"))
    env = EnvSettings()
    runtime = RuntimeConfig(
        config_dir=config_dir,
        study=study,
        discovery=discovery,
        exclusions=exclusions,
        aliases=aliases,
        registry=registry,
        benchmarks=benchmarks,
        entities=entities,
        segments=segments,
        env=env,
    )
    validate_runtime_config(runtime)
    return runtime


def validate_runtime_config(runtime: RuntimeConfig) -> None:
    issues = collect_runtime_config_issues(runtime)
    if not issues:
        return
    formatted = "\n".join(f"- {issue}" for issue in issues)
    raise ConfigValidationError(f"invalid runtime config:\n{formatted}")


def collect_runtime_config_issues(runtime: RuntimeConfig) -> list[str]:
    issues: list[str] = []

    alias_ids = {tech.technology_id for tech in runtime.aliases.technologies}
    registry_ids = {tech.technology_id for tech in runtime.registry.technologies}
    known_technology_ids = alias_ids | registry_ids

    overlapping_technology_ids = sorted(alias_ids & registry_ids)
    if overlapping_technology_ids:
        issues.append(
            "technology ids are duplicated across aliases and registry: "
            + ", ".join(overlapping_technology_ids)
        )

    entity_ids = [entity.entity_id for entity in runtime.entities.entities]
    duplicate_entity_ids = sorted(
        entity_id for entity_id, count in Counter(entity_ids).items() if count > 1
    )
    if duplicate_entity_ids:
        issues.append("entity ids are duplicated: " + ", ".join(duplicate_entity_ids))

    segment_rule_ids = [rule.segment_id for rule in runtime.segments.rules]
    duplicate_segment_rule_ids = sorted(
        segment_id for segment_id, count in Counter(segment_rule_ids).items() if count > 1
    )
    if duplicate_segment_rule_ids:
        issues.append(
            "segment rule ids are duplicated: " + ", ".join(duplicate_segment_rule_ids)
        )

    duplicate_precedence_ids = sorted(
        segment_id
        for segment_id, count in Counter(runtime.segments.precedence).items()
        if count > 1
    )
    if duplicate_precedence_ids:
        issues.append(
            "segment precedence contains duplicates: " + ", ".join(duplicate_precedence_ids)
        )

    unknown_precedence_ids = sorted(set(runtime.segments.precedence) - set(segment_rule_ids))
    if unknown_precedence_ids:
        issues.append(
            "segment precedence references undefined segment rules: "
            + ", ".join(unknown_precedence_ids)
        )

    for rule in runtime.segments.rules:
        unknown_technology_ids = sorted(set(rule.technology_ids) - known_technology_ids)
        if unknown_technology_ids:
            issues.append(
                f"segment rule '{rule.segment_id}' references unknown technology ids: "
                + ", ".join(unknown_technology_ids)
            )

    for entity in runtime.benchmarks.entities:
        technology_ids = entity.technology_ids or (
            [] if entity.expectation == "negative" else [entity.entity_id]
        )
        unknown_technology_ids = sorted(set(technology_ids) - known_technology_ids)
        if unknown_technology_ids:
            issues.append(
                f"benchmark entity '{entity.entity_id}' references unknown technology ids: "
                + ", ".join(unknown_technology_ids)
            )
        for repo_name in entity.repo_names:
            if not _looks_like_full_repo_name(repo_name):
                issues.append(
                    f"benchmark entity '{entity.entity_id}' has invalid repo name '{repo_name}'"
                )

    github_org_to_entity: dict[str, str] = {}
    technology_to_entity: dict[str, str] = {}
    for entity in runtime.entities.entities:
        unknown_technology_ids = sorted(set(entity.technology_ids) - known_technology_ids)
        if unknown_technology_ids:
            issues.append(
                f"entity '{entity.entity_id}' references unknown technology ids: "
                + ", ".join(unknown_technology_ids)
            )
        for repo_name in entity.repo_names:
            if not _looks_like_full_repo_name(repo_name):
                issues.append(
                    f"entity '{entity.entity_id}' has invalid repo name '{repo_name}'"
                )
        for github_org in entity.github_orgs:
            normalized = github_org.casefold()
            existing = github_org_to_entity.get(normalized)
            if existing and existing != entity.entity_id:
                issues.append(
                    f"github org '{github_org}' is mapped to multiple entities: "
                    f"{existing}, {entity.entity_id}"
                )
            github_org_to_entity[normalized] = entity.entity_id
        for technology_id in entity.technology_ids:
            existing = technology_to_entity.get(technology_id)
            if existing and existing != entity.entity_id:
                issues.append(
                    f"technology id '{technology_id}' is mapped to multiple entities: "
                    f"{existing}, {entity.entity_id}"
                )
            technology_to_entity[technology_id] = entity.entity_id

    for repo_name in runtime.discovery.manual_seed_repos:
        if not _looks_like_full_repo_name(repo_name):
            issues.append(f"manual seed repo '{repo_name}' is not in owner/name format")

    return issues


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


def normalized_alias_keys(alias: str) -> set[str]:
    values = {alias.casefold()}
    if "-" in alias:
        values.add(alias.replace("-", "_").casefold())
    if "_" in alias:
        values.add(alias.replace("_", "-").casefold())
    return values


def _looks_like_full_repo_name(repo_name: str) -> bool:
    owner, separator, repo = repo_name.strip().partition("/")
    return bool(separator and owner and repo and "/" not in repo)
