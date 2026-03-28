from __future__ import annotations

from pathlib import Path

import pytest

from oss_ai_stack_map.config.loader import (
    BenchmarkConfig,
    BenchmarkEntity,
    ConfigValidationError,
    DiscoveryConfig,
    EnvSettings,
    ExclusionConfig,
    RuntimeConfig,
    SegmentConfig,
    SegmentRule,
    StudyConfig,
    TechnologyAlias,
    TechnologyAliasConfig,
    load_runtime,
    validate_runtime_config,
)


def _runtime_with_registry_ids(*technology_ids: str) -> RuntimeConfig:
    return RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(),
        discovery=DiscoveryConfig(
            topics=[],
            description_keywords=[],
            manual_seed_repos=["owner/repo"],
        ),
        exclusions=ExclusionConfig(
            hard_keywords=[],
            excluded_directories=[],
            source_extensions=[".py"],
            manifest_files=["pyproject.toml"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id=technology_id,
                    display_name=technology_id,
                    category_id="test-category",
                    aliases=[technology_id],
                )
                for technology_id in technology_ids
            ]
        ),
        benchmarks=BenchmarkConfig(entities=[]),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )


def test_validate_runtime_config_rejects_unknown_segment_technology_ids() -> None:
    runtime = _runtime_with_registry_ids("known-tech")
    runtime.segments = SegmentConfig(
        precedence=["agent_application"],
        rules=[
            SegmentRule(
                segment_id="agent_application",
                technology_ids=["known-tech", "missing-tech"],
            )
        ],
    )

    with pytest.raises(ConfigValidationError, match="missing-tech"):
        validate_runtime_config(runtime)


def test_validate_runtime_config_rejects_unknown_benchmark_technology_ids() -> None:
    runtime = _runtime_with_registry_ids("known-tech")
    runtime.benchmarks = BenchmarkConfig(
        entities=[
            BenchmarkEntity(
                entity_id="benchmark-tech",
                display_name="Benchmark Tech",
                technology_ids=["known-tech", "missing-tech"],
                repo_names=["owner/repo"],
            )
        ]
    )

    with pytest.raises(ConfigValidationError, match="benchmark-tech"):
        validate_runtime_config(runtime)


def test_load_runtime_allows_missing_env_for_offline_commands(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    runtime = load_runtime(Path(__file__).resolve().parents[1] / "config")

    assert runtime.env.github_token is None
    assert runtime.env.openai_api_key is None
