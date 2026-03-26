from __future__ import annotations

from datetime import date
from pathlib import Path

from oss_ai_stack_map.config.loader import (
    ClassificationConfig,
    DiscoveryConfig,
    EnvSettings,
    ExclusionConfig,
    HttpConfig,
    OutputConfig,
    RuntimeConfig,
    SegmentConfig,
    StudyConfig,
    TechnologyAlias,
    TechnologyAliasConfig,
)
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    ManifestDependency,
    RepoContext,
)
from oss_ai_stack_map.pipeline.discovery import normalize_graphql_repo
from oss_ai_stack_map.pipeline.normalize import build_repo_technology_edges


def test_build_repo_technology_edges_prefers_manifest_over_sbom() -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            snapshot_date=date(2026, 3, 25),
            classification=ClassificationConfig(),
            outputs=OutputConfig(write_csv=False),
            http=HttpConfig(),
        ),
        discovery=DiscoveryConfig(topics=[], description_keywords=[], manual_seed_repos=[]),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            excluded_directories=[],
            source_extensions=[".py"],
            manifest_files=["pyproject.toml"],
        ),
        aliases=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="openai",
                    display_name="OpenAI SDK",
                    category_id="model_access_and_providers",
                    provider_id="openai",
                    aliases=["openai"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    context = RepoContext(
        repo_id=1,
        full_name="owner/repo",
        manifest_dependencies=[
            ManifestDependency(
                package_name="openai",
                source_path="pyproject.toml",
                raw_specifier=">=1.0.0",
                evidence_type="manifest",
                technology_id="openai",
                provider_id="openai",
            )
        ],
        sbom_dependencies=[
            ManifestDependency(
                package_name="openai",
                source_path="sbom",
                raw_version="1.55.3",
                evidence_type="sbom",
                purl="pkg:pypi/openai@1.55.3",
                technology_id="openai",
                provider_id="openai",
            )
        ],
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])

    assert len(edges) == 1
    assert edges[0].technology_id == "openai"
    assert edges[0].evidence_type == "manifest"
    assert edges[0].evidence_path == "pyproject.toml"


def test_normalize_graphql_repo_uses_batched_metadata() -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            snapshot_date=date(2026, 3, 25),
            classification=ClassificationConfig(),
            outputs=OutputConfig(write_csv=False),
            http=HttpConfig(),
        ),
        discovery=DiscoveryConfig(topics=[], description_keywords=[], manual_seed_repos=[]),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            educational_keywords=[],
            excluded_directories=[],
            source_extensions=[".py"],
            manifest_files=["pyproject.toml"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    repo = normalize_graphql_repo(
        repo={
            "databaseId": 123,
            "nameWithOwner": "owner/repo",
            "url": "https://github.com/owner/repo",
            "description": "Example repo",
            "isFork": False,
            "isArchived": False,
            "isTemplate": False,
            "stargazerCount": 1234,
            "forkCount": 12,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-02T00:00:00Z",
            "pushedAt": "2026-01-03T00:00:00Z",
            "owner": {"__typename": "Organization", "login": "owner"},
            "primaryLanguage": {"name": "Python"},
            "licenseInfo": {"spdxId": "MIT"},
            "repositoryTopics": {
                "nodes": [{"topic": {"name": "llm"}}, {"topic": {"name": "agents"}}]
            },
        },
        discovery_queries=["topic:llm"],
        runtime=runtime,
    )
    assert repo.repo_id == 123
    assert repo.full_name == "owner/repo"
    assert repo.topics == ["llm", "agents"]
    assert repo.discovery_queries == ["topic:llm"]
