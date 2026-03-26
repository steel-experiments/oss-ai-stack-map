from __future__ import annotations

import json

import pytest

from oss_ai_stack_map.config.loader import TechnologyAlias
from oss_ai_stack_map.models.core import ClassificationDecision, DiscoveredRepo, RepoContext
from oss_ai_stack_map.pipeline.classification import (
    _parse_package_json,
    _parse_pyproject,
    classify_candidates,
    educational_material_signal,
    extract_package_candidates_from_sbom,
    parse_sbom_dependencies,
    score_serious,
)
from oss_ai_stack_map.pipeline.imports import parse_import_dependencies
from oss_ai_stack_map.storage.checkpoints import ClassificationCheckpointStore
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows


def test_parse_pyproject_maps_known_ai_dependency() -> None:
    alias_lookup = {
        "openai": TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            aliases=["openai"],
        )
    }
    text = """
[project]
dependencies = ["openai>=1.0.0", "httpx>=0.28"]
"""
    deps = _parse_pyproject("pyproject.toml", text, alias_lookup)
    assert [dep.package_name for dep in deps] == ["openai", "httpx"]
    assert deps[0].technology_id == "openai"
    assert deps[0].evidence_type == "manifest"


def test_parse_package_json_keeps_dev_scope() -> None:
    alias_lookup = {
        "langgraph": TechnologyAlias(
            technology_id="langgraph",
            display_name="LangGraph",
            category_id="orchestration_and_agents",
            aliases=["langgraph"],
        )
    }
    text = """
{
  "dependencies": {"langgraph": "^1.0.0"},
  "devDependencies": {"typescript": "^5.0.0"}
}
"""
    deps = _parse_package_json("package.json", text, alias_lookup)
    by_name = {dep.package_name: dep for dep in deps}
    assert by_name["langgraph"].technology_id == "langgraph"
    assert by_name["langgraph"].dependency_scope == "runtime"
    assert by_name["typescript"].dependency_scope == "dev"


def test_parse_sbom_dependencies_uses_described_root() -> None:
    alias_lookup = {
        "openai": TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            provider_id="openai",
            aliases=["openai"],
        ),
        "langfuse": TechnologyAlias(
            technology_id="langfuse",
            display_name="Langfuse",
            category_id="observability_tracing_and_monitoring",
            aliases=["langfuse"],
        ),
    }
    sbom = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "packages": [
            {
                "SPDXID": "SPDXRef-root",
                "name": "example-app",
            },
            {
                "SPDXID": "SPDXRef-openai",
                "name": "openai",
                "versionInfo": "1.55.3",
                "licenseConcluded": "Apache-2.0",
                "externalRefs": [
                    {
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/openai@1.55.3",
                    }
                ],
            },
            {
                "SPDXID": "SPDXRef-langfuse",
                "name": "langfuse",
                "versionInfo": "2.0.0",
                "externalRefs": [
                    {
                        "referenceType": "purl",
                        "referenceLocator": "pkg:pypi/langfuse@2.0.0",
                    }
                ],
            },
        ],
        "relationships": [
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relatedSpdxElement": "SPDXRef-root",
                "relationshipType": "DESCRIBES",
            },
            {
                "spdxElementId": "SPDXRef-root",
                "relatedSpdxElement": "SPDXRef-openai",
                "relationshipType": "DEPENDS_ON",
            },
            {
                "spdxElementId": "SPDXRef-root",
                "relatedSpdxElement": "SPDXRef-langfuse",
                "relationshipType": "DEPENDS_ON",
            },
            {
                "spdxElementId": "SPDXRef-openai",
                "relatedSpdxElement": "SPDXRef-langfuse",
                "relationshipType": "DEPENDS_ON",
            },
        ],
    }
    deps = parse_sbom_dependencies(sbom, alias_lookup)
    assert {dep.package_name for dep in deps} == {"openai", "langfuse"}
    assert all(dep.evidence_type == "sbom" for dep in deps)
    assert next(dep for dep in deps if dep.package_name == "openai").provider_id == "openai"


def test_parse_import_dependencies_matches_python_import_aliases() -> None:
    import_lookup = {
        "google.genai": TechnologyAlias(
            technology_id="google-genai",
            display_name="Google GenAI SDK",
            category_id="model_access_and_providers",
            provider_id="google",
            aliases=["google-genai"],
            import_aliases=["google.genai"],
        ),
        "langchain_openai": TechnologyAlias(
            technology_id="langchain-openai",
            display_name="LangChain OpenAI Integration",
            category_id="orchestration_and_agents",
            provider_id="openai",
            aliases=["langchain-openai"],
            import_aliases=["langchain_openai"],
        ),
    }
    text = """
from google import genai
from langchain_openai import ChatOpenAI
"""
    deps = parse_import_dependencies("src/app.py", text, import_lookup)
    assert {dep.technology_id for dep in deps} == {"google-genai", "langchain-openai"}
    assert all(dep.evidence_type == "import" for dep in deps)


def test_extract_package_candidates_from_scoped_npm_purl() -> None:
    package = {"name": "@langchain/openai"}
    candidates = extract_package_candidates_from_sbom(
        package,
        "pkg:npm/%40langchain/openai@0.1.0",
    )
    assert candidates[0] == "@langchain/openai"
    assert "openai" in candidates


def test_educational_material_signal_detects_book_repo(runtime_config) -> None:
    repo = runtime_config["repo_cls"](
        repo_id=1,
        full_name="rasbt/LLMs-from-scratch",
        html_url="https://github.com/rasbt/LLMs-from-scratch",
        stars=1,
        forks=1,
        is_archived=False,
        is_fork=False,
        is_template=False,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        pushed_at="2026-01-01T00:00:00Z",
        snapshot_date="2026-03-25",
        description=(
            "Official code repository for the book Build a Large Language Model "
            "(From Scratch)."
        ),
    )
    assert educational_material_signal(
        runtime_config["runtime"],
        repo,
        "# Build a Large Language Model (From Scratch)\nStep by step companion code.",
    )


def test_educational_material_signal_detects_demonstration_only_repo(runtime_config) -> None:
    repo = runtime_config["repo_cls"](
        repo_id=2,
        full_name="anthropics/skills",
        html_url="https://github.com/anthropics/skills",
        stars=1,
        forks=1,
        is_archived=False,
        is_fork=False,
        is_template=False,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        pushed_at="2026-01-01T00:00:00Z",
        snapshot_date="2026-03-25",
        description="Anthropic implementation of skills for Claude.",
    )
    assert educational_material_signal(
        runtime_config["runtime"],
        repo,
        "Provided for demonstration and educational purposes only.",
    )


def test_score_serious_keeps_single_hard_keyword_exclusion_without_root_manifest(
    runtime_config,
) -> None:
    runtime = runtime_config["runtime"]
    runtime.exclusions.hard_keywords = ["awesome", "demo", "tutorial", "list"]
    repo = runtime_config["repo_cls"](
        repo_id=3,
        full_name="owner/awesome-llm-apps",
        html_url="https://github.com/owner/awesome-llm-apps",
        stars=5000,
        forks=100,
        is_archived=False,
        is_fork=False,
        is_template=False,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        pushed_at="2026-01-01T00:00:00Z",
        snapshot_date="2026-03-25",
        description="Curated collection of LLM apps.",
        topics=["agents"],
        primary_language="Python",
    )
    context = RepoContext(
        repo_id=repo.repo_id,
        full_name=repo.full_name,
        readme_text="Usage examples for many agent apps.",
        tree_paths=[
            ".github/workflows/test.yml",
            ".github/workflows/release.yml",
            "apps/a/app.py",
            "apps/a/requirements.txt",
            "apps/b/main.py",
            "apps/b/requirements.txt",
            "apps/c/worker.py",
            "services/api/server.py",
            "tests/test_repo.py",
            "README.md",
        ],
        manifest_paths=["apps/a/requirements.txt", "apps/b/requirements.txt"],
        manifest_dependencies=[],
        sbom_dependencies=[],
        import_dependencies=[],
    )

    score, notes, exclusion_reason = score_serious(runtime, repo, context)

    assert score >= runtime.study.classification.strong_serious_override_score
    assert "manifest present" in notes
    assert exclusion_reason == "hard exclusion: awesome"


def test_classify_candidates_resumes_from_checkpoints(
    tmp_path, runtime_config, monkeypatch
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    runtime.study.checkpoint_batch_size = 2
    runtime.aliases.technologies = [
        TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            aliases=["openai"],
        )
    ]
    output_dir = tmp_path / "run"
    repos = [
        DiscoveredRepo(
            repo_id=repo_id,
            full_name=f"owner/repo-{repo_id}",
            html_url=f"https://github.com/owner/repo-{repo_id}",
            stars=1000 + repo_id,
            forks=10,
            is_archived=False,
            is_fork=False,
            is_template=False,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T00:00:00Z",
            pushed_at="2026-01-01T00:00:00Z",
            snapshot_date="2026-03-25",
            description=f"repo {repo_id}",
        )
        for repo_id in [1, 2, 3]
    ]
    write_rows(output_dir, "repos", [repo.to_row() for repo in repos], write_csv=False)

    first_run_ids: list[int] = []
    second_run_ids: list[int] = []
    failing_repo_id = 3
    should_fail = {"value": True}

    def fake_build_repo_context(runtime, client, repo, alias_lookup):
        if should_fail["value"] and repo.repo_id == failing_repo_id:
            raise RuntimeError("simulated crash")
        target = first_run_ids if should_fail["value"] else second_run_ids
        target.append(repo.repo_id)
        return RepoContext(
            repo_id=repo.repo_id,
            full_name=repo.full_name,
            manifest_dependencies=[],
            sbom_dependencies=[],
            import_dependencies=[],
        )

    def fake_classify_repo(runtime, repo, context, alias_lookup):
        include = repo.repo_id != 2
        return ClassificationDecision(
            repo_id=repo.repo_id,
            full_name=repo.full_name,
            passed_candidate_filter=True,
            passed_serious_filter=include,
            passed_ai_relevance_filter=include,
            passed_major_filter=include,
            score_serious=5 if include else 1,
            score_ai=5 if include else 1,
            primary_segment="serving_runtime" if include else None,
            notes=[],
        )

    monkeypatch.setattr(
        "oss_ai_stack_map.pipeline.classification.build_repo_context",
        fake_build_repo_context,
    )
    monkeypatch.setattr(
        "oss_ai_stack_map.pipeline.classification.classify_repo",
        fake_classify_repo,
    )

    with pytest.raises(RuntimeError, match="simulated crash"):
        classify_candidates(
            runtime=runtime,
            client=object(),
            input_dir=output_dir,
            output_dir=output_dir,
        )

    checkpoint_decisions = read_parquet_models(
        output_dir / "checkpoints" / "repo_inclusion_decisions" / "part-00001.parquet",
        ClassificationDecision,
    )
    assert [decision.repo_id for decision in checkpoint_decisions] == [1, 2]

    should_fail["value"] = False
    summary = classify_candidates(
        runtime=runtime,
        client=object(),
        input_dir=output_dir,
        output_dir=output_dir,
    )

    assert first_run_ids == [1, 2]
    assert second_run_ids == [3]
    assert summary.total == 3
    assert summary.passed_major == 2

    final_decisions = read_parquet_models(
        output_dir / "repo_inclusion_decisions.parquet",
        ClassificationDecision,
    )
    assert [decision.repo_id for decision in final_decisions] == [1, 2, 3]

    with (output_dir / "checkpoints" / "run_state.json").open("r", encoding="utf-8") as handle:
        run_state = json.load(handle)
    assert run_state["status"] == "completed"
    assert run_state["processed_repo_count"] == 3


def test_classify_candidates_skips_large_rewrites_on_finalize_only(
    tmp_path, runtime_config, monkeypatch
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    output_dir = tmp_path / "run"
    repos = [
        DiscoveredRepo(
            repo_id=1,
            full_name="owner/repo-1",
            html_url="https://github.com/owner/repo-1",
            stars=1001,
            forks=10,
            is_archived=False,
            is_fork=False,
            is_template=False,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T00:00:00Z",
            pushed_at="2026-01-01T00:00:00Z",
            snapshot_date="2026-03-25",
            description="repo",
        )
    ]
    write_rows(output_dir, "repos", [repo.to_row() for repo in repos], write_csv=False)
    write_rows(
        output_dir,
        "repo_contexts",
        [
            RepoContext(
                repo_id=1,
                full_name="owner/repo-1",
                manifest_dependencies=[],
                sbom_dependencies=[],
                import_dependencies=[],
            ).to_row()
        ],
        write_csv=False,
    )
    checkpoint_dir = output_dir / "checkpoints"
    write_rows(
        checkpoint_dir / "repo_contexts",
        "part-00001",
        [
            RepoContext(
                repo_id=1,
                full_name="owner/repo-1",
                manifest_dependencies=[],
                sbom_dependencies=[],
                import_dependencies=[],
            ).to_row()
        ],
        write_csv=False,
    )
    write_rows(
        checkpoint_dir / "repo_inclusion_decisions",
        "part-00001",
        [
            ClassificationDecision(
                repo_id=1,
                full_name="owner/repo-1",
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=5,
                score_ai=5,
                primary_segment="serving_runtime",
                notes=[],
            ).to_row()
        ],
        write_csv=False,
    )
    store = ClassificationCheckpointStore(output_dir, write_csv=False)
    state = store.ensure_compatible_run(runtime=runtime, repo_ids=[1])
    state.update(
        {
            "status": "completed",
            "stage": "completed",
            "processed_repo_count": 1,
            "remaining_repo_count": 0,
            "completed_checkpoint_batches": 1,
        }
    )
    store.save_run_state(state)

    original_context_mtime = (output_dir / "repo_contexts.parquet").stat().st_mtime_ns

    summary = classify_candidates(
        runtime=runtime,
        client=object(),
        input_dir=output_dir,
        output_dir=output_dir,
    )

    assert summary.total == 1
    assert (output_dir / "repo_contexts.parquet").stat().st_mtime_ns == original_context_mtime
