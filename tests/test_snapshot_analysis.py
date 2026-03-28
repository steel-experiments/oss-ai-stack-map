from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from oss_ai_stack_map.analysis.snapshot import (
    append_experiment_ledger_entry,
    compare_snapshots,
    refresh_snapshot_contexts,
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
    assert (output_dir / "technology_discovery_report.json").exists()
    assert (output_dir / "registry_suggestions.json").exists()

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
    scorecard_by_metric = {row["metric"]: row for row in comparison["scorecard"]["metrics"]}
    assert scorecard_by_metric["final_repos"]["status"] == "improved"
    assert scorecard_by_metric["judge_decisions"]["status"] == "unchanged"


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


def test_validate_snapshot_warns_on_benchmark_threshold_failures(tmp_path: Path) -> None:
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
    (input_dir / "benchmark_recall_report.json").write_text(
        json.dumps(
            {
                "entity_count": 2,
                "entities_with_repo_discovered": 1,
                "entities_with_repo_included": 0,
                "entities_with_repo_identity_mapped": 0,
                "entities_with_third_party_adoption": 0,
                "entities_with_dependency_evidence": 0,
                "failed_thresholds": [
                    {
                        "metric": "repo_discovered_rate",
                        "actual": 0.5,
                        "minimum": 1.0,
                        "severity": "warning",
                    }
                ],
                "prioritized_gaps": [],
                "entities": [],
            }
        ),
        encoding="utf-8",
    )

    validation = validate_snapshot(input_dir)

    assert validation["status"] == "warning"
    assert any("benchmark recall threshold failed" in item for item in validation["warnings"])


def test_compare_snapshots_scores_benchmark_and_gap_deltas_and_logs_experiment(
    runtime_config, tmp_path: Path
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
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

    repo = _build_repo(1, description="An AI assistant SDK", topics=["llm"])
    context = RepoContext(
        repo_id=1,
        full_name=repo.full_name,
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

    base_dir = tmp_path / "base"
    write_rows(base_dir, "repos", [repo.to_row()], write_csv=False)
    write_rows(base_dir, "repo_contexts", [context.to_row()], write_csv=False)
    write_rows(
        base_dir,
        "repo_inclusion_decisions",
        [
            ClassificationDecision(
                repo_id=1,
                full_name=repo.full_name,
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=6,
                score_ai=6,
            ).to_row()
        ],
        write_csv=False,
    )
    write_rows(
        base_dir,
        "repo_technology_edges",
        [
            {
                "repo_id": 1,
                "full_name": repo.full_name,
                "technology_id": "openai",
                "raw_signal": "openai",
                "evidence_type": "manifest",
                "evidence_path": "pyproject.toml",
                "snapshot_date": "2026-03-25",
            }
        ],
        write_csv=False,
    )
    write_rows(
        base_dir,
        "technologies",
        [
            {
                "technology_id": "openai",
                "display_name": "OpenAI SDK",
                "category_id": "model_access_and_providers",
            }
        ],
        write_csv=False,
    )
    (base_dir / "gap_report.json").write_text(
        json.dumps(
            {
                "final_repos_missing_edges_count": 2,
                "final_repos_missing_edges_with_unmapped_dependency_evidence_count": 1,
                "final_repos_missing_edges_with_no_dependency_evidence_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (base_dir / "benchmark_recall_report.json").write_text(
        json.dumps(
            {
                "repo_discovered_rate": 0.5,
                "repo_included_rate": 0.5,
                "repo_identity_mapped_rate": 0.5,
                "third_party_adoption_rate": 0.5,
                "dependency_evidence_rate": 0.5,
                "failed_thresholds": [{"metric": "repo_discovered_rate"}],
            }
        ),
        encoding="utf-8",
    )

    candidate_dir = tmp_path / "candidate"
    shutil.copytree(base_dir, candidate_dir)
    (candidate_dir / "gap_report.json").write_text(
        json.dumps(
            {
                "final_repos_missing_edges_count": 0,
                "final_repos_missing_edges_with_unmapped_dependency_evidence_count": 0,
                "final_repos_missing_edges_with_no_dependency_evidence_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (candidate_dir / "benchmark_recall_report.json").write_text(
        json.dumps(
            {
                "repo_discovered_rate": 1.0,
                "repo_included_rate": 1.0,
                "repo_identity_mapped_rate": 1.0,
                "third_party_adoption_rate": 1.0,
                "dependency_evidence_rate": 1.0,
                "failed_thresholds": [],
            }
        ),
        encoding="utf-8",
    )

    comparison = compare_snapshots(base_dir, candidate_dir)
    scorecard_by_metric = {row["metric"]: row for row in comparison["scorecard"]["metrics"]}
    assert scorecard_by_metric["repo_discovered_rate"]["status"] == "improved"
    assert (
        scorecard_by_metric[
            "final_repos_missing_edges_with_unmapped_dependency_evidence_count"
        ]["status"]
        == "improved"
    )

    ledger_path = tmp_path / "experiments" / "ledger.jsonl"
    entry = append_experiment_ledger_entry(
        ledger_path,
        report=comparison,
        lever="benchmark-and-gap-hardening",
        files_changed=[
            "config/technology_registry.yaml",
            "src/oss_ai_stack_map/analysis/snapshot.py",
        ],
        decision="keep",
        note="Improved benchmark and gap scorecard metrics.",
    )
    assert entry["decision"] == "keep"
    logged = ledger_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(logged) == 1
    assert json.loads(logged[0])["lever"] == "benchmark-and-gap-hardening"


def test_repair_snapshot_rebinds_dependency_matches_from_current_registry(
    runtime_config,
    tmp_path: Path,
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    runtime.discovery.topics = ["llm"]
    runtime.discovery.description_keywords = ["agent"]
    runtime.registry.technologies = [
        TechnologyAlias(
            technology_id="mastra",
            display_name="Mastra",
            category_id="orchestration_and_agents",
            entity_type="product",
            aliases=["mastra"],
            package_prefixes=["@mastra/"],
            repo_names=["mastra-ai/mastra"],
        )
    ]

    repo = DiscoveredRepo(
        repo_id=1,
        full_name="custom/app",
        html_url="https://github.com/custom/app",
        description="AI agent app",
        owner_type="Organization",
        stars=5000,
        forks=50,
        primary_language="TypeScript",
        topics=["llm", "agents"],
        license_spdx="MIT",
        is_archived=False,
        is_fork=False,
        is_template=False,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        pushed_at="2026-01-01T00:00:00Z",
        snapshot_date="2026-03-25",
    )
    context = RepoContext(
        repo_id=1,
        full_name=repo.full_name,
        readme_text="## Installation\n## Usage",
        tree_paths=_build_tree_paths(),
        manifest_paths=["package.json"],
        manifest_dependencies=[
            ManifestDependency(
                package_name="@mastra/core",
                source_path="package.json",
                raw_specifier="^1.0.0",
                technology_id=None,
            )
        ],
    )

    input_dir = tmp_path / "input"
    write_rows(input_dir, "repos", [repo.to_row()], write_csv=False)
    write_rows(input_dir, "repo_contexts", [context.to_row()], write_csv=False)
    write_rows(
        input_dir,
        "repo_inclusion_decisions",
        [
            ClassificationDecision(
                repo_id=1,
                full_name=repo.full_name,
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=6,
                score_ai=6,
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
                "status": "completed",
                "stage": "completed",
                "processed_repo_count": 1,
                "total_repos": 1,
                "remaining_repo_count": 0,
                "started_at": "2026-03-25T00:00:00+00:00",
                "updated_at": "2026-03-25T01:00:00+00:00",
            },
            handle,
            indent=2,
            sort_keys=True,
        )

    output_dir = tmp_path / "repaired"
    repair_snapshot(runtime=runtime, input_dir=input_dir, output_dir=output_dir)

    repaired_contexts = read_parquet_models(output_dir / "repo_contexts.parquet", RepoContext)
    assert repaired_contexts[0].manifest_dependencies[0].technology_id == "mastra"
    assert repaired_contexts[0].manifest_dependencies[0].match_method == "package_prefix"

    dependency_rows = read_parquet_models(
        output_dir / "repo_dependency_evidence.parquet",
        ManifestDependency,
    )
    assert dependency_rows[0].technology_id == "mastra"

    edge_rows = pq.read_table(
        output_dir / "repo_technology_edges.parquet",
        columns=["technology_id"],
    ).to_pylist()
    assert {"technology_id": "mastra"} in edge_rows


def test_refresh_snapshot_contexts_rebuilds_selected_empty_tree_contexts(
    runtime_config,
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False

    repo1 = _build_repo(1, description="An AI assistant SDK", topics=["llm"])
    repo2 = _build_repo(2, description="An AI assistant shell", topics=["llm"])
    repo1 = repo1.model_copy(update={"default_branch": "main"})
    repo2 = repo2.model_copy(update={"default_branch": "main"})

    input_dir = tmp_path / "input"
    write_rows(input_dir, "repos", [repo1.to_row(), repo2.to_row()], write_csv=False)
    write_rows(
        input_dir,
        "repo_contexts",
        [
            RepoContext(
                repo_id=1,
                full_name=repo1.full_name,
                default_branch="HEAD",
                tree_paths=[],
                manifest_paths=[],
            ).to_row(),
            RepoContext(
                repo_id=2,
                full_name=repo2.full_name,
                default_branch="main",
                tree_paths=_build_tree_paths(),
                manifest_paths=[],
            ).to_row(),
        ],
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
                passed_serious_filter=False,
                passed_ai_relevance_filter=True,
                passed_major_filter=False,
                score_serious=0,
                score_ai=8,
            ).to_row(),
            ClassificationDecision(
                repo_id=2,
                full_name=repo2.full_name,
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=6,
                score_ai=6,
            ).to_row(),
        ],
        write_csv=False,
    )
    checkpoints_dir = input_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    with (checkpoints_dir / "run_state.json").open("w", encoding="utf-8") as handle:
        json.dump({"status": "completed", "stage": "completed"}, handle)

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    def fake_build_repo_context(runtime, client, repo, alias_lookup):
        assert repo.full_name == repo1.full_name
        return RepoContext(
            repo_id=repo.repo_id,
            full_name=repo.full_name,
            default_branch=repo.default_branch,
            readme_text="## Installation\n## Usage",
            tree_paths=_build_tree_paths(),
            manifest_paths=["pyproject.toml"],
            manifest_dependencies=[],
            sbom_dependencies=[],
            import_dependencies=[],
        )

    monkeypatch.setattr(
        "oss_ai_stack_map.analysis.snapshot.GitHubClient",
        lambda runtime: FakeClient(),
    )
    monkeypatch.setattr(
        "oss_ai_stack_map.analysis.snapshot.build_repo_context",
        fake_build_repo_context,
    )

    output_dir = tmp_path / "refreshed"
    summary = refresh_snapshot_contexts(
        runtime=runtime,
        input_dir=input_dir,
        output_dir=output_dir,
        min_ai_score=6,
    )

    assert summary["refreshed_repo_count"] == 1
    refreshed_contexts = read_parquet_models(output_dir / "repo_contexts.parquet", RepoContext)
    by_repo = {context.repo_id: context for context in refreshed_contexts}
    assert by_repo[1].default_branch == "main"
    assert by_repo[1].manifest_paths == ["pyproject.toml"]
