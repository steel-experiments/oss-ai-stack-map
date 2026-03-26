from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FilterConfig(BaseModel):
    candidate_stars_min: int = 1000
    major_stars_min: int = 1000
    freshness_months: int = 12
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
    override_on_high_confidence: bool = True
    min_confidence_to_override: str = "high"
    reasoning_effort: str = "low"

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
    aliases: list[str]
    import_aliases: list[str] = Field(default_factory=list)


class TechnologyAliasConfig(BaseModel):
    technologies: list[TechnologyAlias]

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


class SegmentRule(BaseModel):
    segment_id: str
    topic_keywords: list[str] = Field(default_factory=list)
    description_keywords: list[str] = Field(default_factory=list)
    technology_ids: list[str] = Field(default_factory=list)
    config_keywords: list[str] = Field(default_factory=list)


class SegmentConfig(BaseModel):
    precedence: list[str]
    rules: list[SegmentRule]


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    github_token: str
    openai_api_key: str | None = None


class RuntimeConfig(BaseModel):
    config_dir: Path
    study: StudyConfig
    discovery: DiscoveryConfig
    exclusions: ExclusionConfig
    aliases: TechnologyAliasConfig
    segments: SegmentConfig
    env: EnvSettings


def load_runtime(config_dir: Path = Path("config")) -> RuntimeConfig:
    config_dir = config_dir.resolve()
    study = StudyConfig.model_validate(_load_yaml(config_dir / "study_config.yaml"))
    discovery = DiscoveryConfig.model_validate(_load_yaml(config_dir / "discovery_topics.yaml"))
    exclusions = ExclusionConfig.model_validate(_load_yaml(config_dir / "exclusion_rules.yaml"))
    aliases = TechnologyAliasConfig.model_validate(
        _load_yaml(config_dir / "technology_aliases.yaml")
    )
    segments = SegmentConfig.model_validate(_load_yaml(config_dir / "segment_rules.yaml"))
    env = EnvSettings()
    return RuntimeConfig(
        config_dir=config_dir,
        study=study,
        discovery=discovery,
        exclusions=exclusions,
        aliases=aliases,
        segments=segments,
        env=env,
    )


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
