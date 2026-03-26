from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from oss_ai_stack_map.analysis.snapshot import (
    compare_snapshots,
    repair_snapshot,
    validate_snapshot,
    write_snapshot_docs,
)
from oss_ai_stack_map.config.loader import TechnologyAlias
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    DiscoveredRepo,
    JudgeDecision,
    ManifestDependency,
    RepoContext,
)
from oss_ai_stack_map.pipeline.classification import classify_repo
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows


def _build_repo(repo_id: int, *, description: str, topics: list[str]) -> DiscoveredRepo:
    return DiscoveredRepo(
        repo_id=repo_id,
        full_name=f"owner/repo-{repo_id}",
        html_url=f"https://github.com/owner/repo-{repo_id}",
        stars=1000 + repo_id,
        forks=10 + repo_id,
        is_archived=False,
        is_fork=False,
        is_template=False,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        pushed_at="2026-01-01T00:00:00Z",
        snapshot_date="2026-03-25",
        description=description,
        topics=topics,
        primary_language="Python",
        owner_type="Organization",
    )


def _build_tree_paths() -> list[str]:
    return [
        "src/a.py",
        "src/b.py",
        "src/c.py",
        "src/d.py",
        "src/e.py",
        "tests/test_app.py",
        ".github/workflows/ci.yml",
    ]


def test_classify_repo_sets_rule_baseline_fields(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.discovery.topics = ["llm"]
    runtime.discovery.description_keywords = ["ai assistant"]
    runtime.aliases.technologies = []
    repo = _build_repo(1, description="An AI assistant", topics=["llm"])
    context = RepoContext(
        repo_id=1,
        full_name=repo.full_name,
        readme_text="## Installation\n## Usage",
        tree_paths=_build_tree_paths(),
        manifest_paths=[],
    )

    decision = classify_repo(runtime=runtime, repo=repo, context=context, alias_lookup={})

    assert decision.rule_passed_serious_filter is True
    assert decision.rule_passed_ai_relevance_filter is False
    assert decision.rule_passed_major_filter is False


def test_validate_repair_and_compare_snapshot(runtime_config, tmp_path: Path) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    runtime.discovery.topics = ["llm"]
    runtime.discovery.description_keywords = ["ai assistant"]
    runtime.aliases.technologies = [
        TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            provider_id="openai",
            aliases=["openai"],
            import_aliases=["openai"],
        )
    ]

    repo1 = _build_repo(1, description="An AI assistant SDK", topics=["llm"])
    repo2 = _build_repo(2, description="An AI assistant shell", topics=["llm"])
    repos = [repo1, repo2]

    context1 = RepoContext(
        repo_id=1,
        full_name=repo1.full_name,
        readme_text="## Installation\n## Usage",
        tree_paths=_build_tree_paths(),
        manifest_paths=["pyproject.toml"],
        manifest_dependencies=[
            ManifestDependency(
                package_name="openai",
                source_path="pyproject.toml",
                raw_specifier=">=1.0.0",
                technology_id="openai",
                provider_id="openai",
            )
        ],
    )
    context2 = RepoContext(
        repo_id=2,
        full_name=repo2.full_name,
        readme_text="## Installation\n## Usage",
        tree_paths=_build_tree_paths(),
        manifest_paths=[],
    )

    input_dir = tmp_path / "input"
    write_rows(input_dir, "repos", [repo.to_row() for repo in repos], write_csv=False)
    write_rows(
        input_dir,
        "repo_contexts",
        [context1.to_row(), context2.to_row()],
        write_csv=False,
    )
    write_rows(
        input_dir,
        "repo_inclusion_decisions",
        [
            ClassificationDecision(
                repo_id=1,
                full_name=repo1.full_name,
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=6,
                score_ai=6,
            ).to_row(),
            ClassificationDecision(
                repo_id=2,
                full_name=repo2.full_name,
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=False,
                passed_major_filter=False,
                score_serious=6,
                score_ai=2,
            ).to_row(),
        ],
        write_csv=False,
    )
    write_rows(
        input_dir,
        "judge_decisions",
        [
            JudgeDecision(
                repo_id=2,
                full_name=repo2.full_name,
                serious_project=True,
                ai_relevant=True,
                include_in_final_set=True,
                primary_segment="agent_application",
                confidence="high",
                override_rule_decision=True,
                reasons=["Clearly an AI product"],
                model="gpt-5.4-nano",
                applied=True,
            ).to_row()
        ],
        write_csv=False,
    )
    checkpoints_dir = input_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    with (checkpoints_dir / "run_state.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "command": "classification",
                "status": "running",
                "stage": "classification_finalize",
                "processed_repo_count": 2,
                "total_repos": 2,
                "remaining_repo_count": 0,
                "started_at": "2026-03-25T00:00:00+00:00",
                "updated_at": "2026-03-25T01:00:00+00:00",
                "attempt_id": "attempt-001",
            },
            handle,
            indent=2,
            sort_keys=True,
        )

    validation = validate_snapshot(input_dir)
    assert validation["status"] == "warning"
    assert any("run_state.json is not completed" in item for item in validation["warnings"])
    assert any("rule_passed_*" in item for item in validation["warnings"])

    output_dir = tmp_path / "repaired"
    summary = repair_snapshot(runtime=runtime, input_dir=input_dir, output_dir=output_dir)
    assert summary["final_repo_count"] == 2
    assert summary["judge_changed_final_repo_ids"] == [2]

    repaired_decisions = read_parquet_models(
        output_dir / "repo_inclusion_decisions.parquet",
        ClassificationDecision,
    )
    by_repo = {decision.repo_id: decision for decision in repaired_decisions}
    assert by_repo[1].rule_passed_major_filter is True
    assert by_repo[2].rule_passed_major_filter is False
    assert by_repo[2].passed_major_filter is True
    assert by_repo[2].judge_override_applied is True

    repaired_validation = validate_snapshot(output_dir)
    assert repaired_validation["status"] == "ok"
    assert not any("rule_passed_*" in item for item in repaired_validation["warnings"])

    docs_dir = tmp_path / "docs"
    paths = write_snapshot_docs(input_dir=output_dir, docs_dir=docs_dir)
    assert Path(paths["summary"]).exists()
    assert Path(paths["descriptive"]).exists()
    assert Path(paths["validation"]).exists()

    comparison = compare_snapshots(input_dir, output_dir)
    assert comparison["changed_final_repo_ids"] == [2]
    assert comparison["metric_deltas"]["final_repos"] == 1


def test_validate_snapshot_treats_unmapped_dependency_evidence_as_non_fatal(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    write_rows(
        input_dir,
        "repos",
        [
            _build_repo(
                1,
                description="AI runtime",
                topics=["llm"],
            ).to_row()
        ],
        write_csv=False,
    )
    write_rows(
        input_dir,
        "repo_contexts",
        [
            RepoContext(
                repo_id=1,
                full_name="owner/repo-1",
                manifest_dependencies=[
                    ManifestDependency(
                        package_name="serde",
                        source_path="Cargo.toml",
                        raw_specifier="1.0",
                    )
                ],
                sbom_dependencies=[],
                import_dependencies=[],
            ).to_row()
        ],
        write_csv=False,
    )
    write_rows(
        input_dir,
        "repo_inclusion_decisions",
        [
            ClassificationDecision(
                repo_id=1,
                full_name="owner/repo-1",
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                rule_passed_serious_filter=True,
                rule_passed_ai_relevance_filter=True,
                rule_passed_major_filter=True,
                score_serious=5,
                score_ai=5,
            ).to_row()
        ],
        write_csv=False,
    )
    write_rows(
        input_dir,
        "repo_dependency_evidence",
        [
                ManifestDependency(
                    package_name="serde",
                    source_path="Cargo.toml",
                    raw_specifier="1.0",
                ).to_row(repo_id=1, snapshot_date=date(2026, 3, 25))
            ],
            write_csv=False,
        )

    validation = validate_snapshot(input_dir)

    assert validation["status"] == "ok"
    assert validation["metrics"]["final_repos_missing_edges"] == 1
    assert validation["metrics"]["final_repos_with_only_unmapped_dependency_evidence"] == 1
    assert validation["metrics"]["final_repos_with_mapped_dependency_evidence_but_no_edge"] == 0
