from __future__ import annotations

from pathlib import Path

from oss_ai_stack_map.config.loader import BenchmarkEntity, TechnologyAlias
from oss_ai_stack_map.pipeline.discovery import (
    build_query_specs,
    derived_seed_repos,
    discover_candidates,
    select_preflight_repositories,
)


class FakeGitHubClient:
    def search_repositories(self, query: str, page: int) -> dict:
        if query == "repo:daytonaio/daytona" and page == 1:
            return {
                "items": [
                    {
                        "id": 753490180,
                        "full_name": "daytonaio/daytona",
                        "html_url": "https://github.com/daytonaio/daytona",
                        "description": (
                            "Daytona is a Secure and Elastic Infrastructure "
                            "for Running AI-Generated Code"
                        ),
                        "owner": {"type": "Organization"},
                        "stargazers_count": 70785,
                        "forks_count": 5517,
                        "language": "TypeScript",
                        "topics": ["ai", "ai-runtime"],
                        "license": {"spdx_id": "AGPL-3.0"},
                        "archived": False,
                        "fork": False,
                        "is_template": False,
                        "created_at": "2024-02-06T08:21:20Z",
                        "updated_at": "2026-03-26T16:10:11Z",
                        "pushed_at": "2026-03-26T15:42:06Z",
                    }
                ]
            }
        return {"items": []}

    def get_repositories_metadata(self, full_names: list[str]) -> dict[str, dict]:
        assert full_names == ["daytonaio/daytona"]
        return {
            "daytonaio/daytona": {
                "databaseId": 753490180,
                "nameWithOwner": "daytonaio/daytona",
                "url": "https://github.com/daytonaio/daytona",
                "description": (
                    "Daytona is a Secure and Elastic Infrastructure "
                    "for Running AI-Generated Code"
                ),
                "isFork": False,
                "isArchived": False,
                "isTemplate": False,
                "stargazerCount": 70785,
                "forkCount": 5517,
                "createdAt": "2024-02-06T08:21:20Z",
                "updatedAt": "2026-03-26T16:10:11Z",
                "pushedAt": "2026-03-26T15:42:06Z",
                "owner": {"__typename": "Organization", "login": "daytonaio"},
                "primaryLanguage": {"name": "TypeScript"},
                "licenseInfo": {"spdxId": "AGPL-3.0"},
                "repositoryTopics": {
                    "nodes": [
                        {"topic": {"name": "ai"}},
                        {"topic": {"name": "ai-runtime"}},
                    ]
                },
            }
        }


def test_discover_candidates_includes_manual_seed_repo_when_reachable(
    runtime_config, tmp_path: Path
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    runtime.discovery.manual_seed_repos = ["daytonaio/daytona"]

    result = discover_candidates(
        runtime=runtime,
        client=FakeGitHubClient(),
        output_dir=tmp_path,
    )

    assert [repo.full_name for repo in result.repositories] == ["daytonaio/daytona"]
    assert result.repositories[0].discovery_queries == ["repo:daytonaio/daytona"]
    assert result.repositories[0].discovery_source_types == ["manual_seed"]


def test_build_query_specs_derives_exact_seeds_from_registry_and_benchmarks(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.discovery.topics = ["llm"]
    runtime.discovery.description_keywords = ["agent"]
    runtime.discovery.manual_seed_repos = ["owner/manual"]
    runtime.registry.technologies = [
        TechnologyAlias(
            technology_id="registry-tech",
            display_name="Registry Tech",
            category_id="test-category",
            aliases=["registry-tech"],
            repo_names=["owner/registry"],
        )
    ]
    runtime.benchmarks.entities = [
        BenchmarkEntity(
            entity_id="benchmark-tech",
            display_name="Benchmark Tech",
            repo_names=["owner/benchmark"],
        )
    ]

    seed_sources = derived_seed_repos(runtime)
    query_specs = build_query_specs(runtime)

    assert seed_sources["owner/manual"] == {"manual_seed"}
    assert seed_sources["owner/registry"] == {"registry_seed"}
    assert seed_sources["owner/benchmark"] == {"benchmark_seed"}
    assert ("repo:owner/manual", "repo_seed") in query_specs
    assert ("repo:owner/registry", "repo_seed") in query_specs
    assert ("repo:owner/benchmark", "repo_seed") in query_specs


def test_discover_candidates_prioritizes_seed_queries_before_broad_search_when_capped(
    runtime_config, tmp_path: Path
) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.outputs.write_csv = False
    runtime.study.filters.max_repos = 1
    runtime.discovery.topics = ["llm"]
    runtime.discovery.manual_seed_repos = ["seed/repo"]

    class SeedFirstClient:
        def search_repositories(self, query: str, page: int) -> dict:
            if query == "repo:seed/repo":
                return {
                    "items": [
                        {
                            "id": 2,
                            "full_name": "seed/repo",
                            "html_url": "https://github.com/seed/repo",
                            "description": "Seed repo",
                            "owner": {"type": "Organization"},
                            "stargazers_count": 50,
                            "forks_count": 5,
                            "language": "Python",
                            "topics": ["llm"],
                            "license": {"spdx_id": "MIT"},
                            "archived": False,
                            "fork": False,
                            "is_template": False,
                            "created_at": "2026-01-01T00:00:00Z",
                            "updated_at": "2026-01-01T00:00:00Z",
                            "pushed_at": "2026-01-01T00:00:00Z",
                            "default_branch": "main",
                        }
                    ]
                }
            if query.startswith("topic:"):
                return {
                    "items": [
                        {
                            "id": 1,
                            "full_name": "topic/repo",
                            "html_url": "https://github.com/topic/repo",
                            "description": "Broad search repo",
                            "owner": {"type": "Organization"},
                            "stargazers_count": 100,
                            "forks_count": 10,
                            "language": "Python",
                            "topics": ["llm"],
                            "license": {"spdx_id": "MIT"},
                            "archived": False,
                            "fork": False,
                            "is_template": False,
                            "created_at": "2026-01-01T00:00:00Z",
                            "updated_at": "2026-01-01T00:00:00Z",
                            "pushed_at": "2026-01-01T00:00:00Z",
                            "default_branch": "main",
                        }
                    ]
                }
            return {"items": []}

        def get_repositories_metadata(self, full_names: list[str]) -> dict[str, dict]:
            return {}

    result = discover_candidates(
        runtime=runtime,
        client=SeedFirstClient(),
        output_dir=tmp_path,
    )

    assert [repo.full_name for repo in result.repositories] == ["seed/repo"]
    assert result.repositories[0].discovery_source_types == ["manual_seed"]


def test_derived_seed_repos_marks_anchor_registry_and_alias_repos(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.discovery.manual_seed_repos = []
    runtime.aliases.technologies = [
        TechnologyAlias(
            technology_id="openai",
            display_name="OpenAI SDK",
            category_id="model_access_and_providers",
            provider_id="openai",
            aliases=["openai"],
            repo_names=["openai/openai-python"],
        )
    ]
    runtime.registry.technologies = [
        TechnologyAlias(
            technology_id="vercel-ai-sdk",
            display_name="Vercel AI SDK",
            category_id="ai_developer_tools_and_sdk_families",
            aliases=["ai-sdk"],
            repo_names=["vercel/ai"],
            capabilities=["model_access"],
        )
    ]

    seed_sources = derived_seed_repos(runtime)

    assert seed_sources["openai/openai-python"] == {"alias_seed", "anchor_seed"}
    assert seed_sources["vercel/ai"] == {"anchor_seed", "registry_seed"}


def test_select_preflight_repositories_prioritizes_benchmark_repos(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    repo_cls = runtime_config["repo_cls"]
    runtime.benchmarks.entities = [
        BenchmarkEntity(
            entity_id="vercel-ai-sdk",
            display_name="Vercel AI SDK",
            repo_names=["vercel/ai"],
        )
    ]
    repositories = [
        repo_cls(
            repo_id=1,
            full_name="top/repo",
            html_url="https://github.com/top/repo",
            stars=10000,
            forks=100,
            is_archived=False,
            is_fork=False,
            is_template=False,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-02T00:00:00Z",
            pushed_at="2026-01-03T00:00:00Z",
            snapshot_date=runtime.study.snapshot_date,
        ),
        repo_cls(
            repo_id=2,
            full_name="vercel/ai",
            html_url="https://github.com/vercel/ai",
            stars=5000,
            forks=50,
            is_archived=False,
            is_fork=False,
            is_template=False,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-02T00:00:00Z",
            pushed_at="2026-01-03T00:00:00Z",
            snapshot_date=runtime.study.snapshot_date,
        ),
        repo_cls(
            repo_id=3,
            full_name="next/repo",
            html_url="https://github.com/next/repo",
            stars=9000,
            forks=80,
            is_archived=False,
            is_fork=False,
            is_template=False,
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-02T00:00:00Z",
            pushed_at="2026-01-03T00:00:00Z",
            snapshot_date=runtime.study.snapshot_date,
        ),
    ]

    sampled = select_preflight_repositories(runtime, repositories, sample_size=2)

    assert [repo.full_name for repo in sampled] == ["vercel/ai", "top/repo"]
