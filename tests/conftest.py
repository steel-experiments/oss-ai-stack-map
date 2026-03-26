from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

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
    TechnologyAliasConfig,
)
from oss_ai_stack_map.models.core import DiscoveredRepo


@pytest.fixture
def runtime_config():
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
            educational_keywords=[
                "from scratch",
                "official code repository for the book",
                "step by step",
            ],
            excluded_directories=[],
            source_extensions=[".py"],
            manifest_files=["pyproject.toml"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    return {"runtime": runtime, "repo_cls": DiscoveredRepo}
