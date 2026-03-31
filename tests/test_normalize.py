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
from oss_ai_stack_map.pipeline.classification import (
    parse_manifest_dependencies,
    parse_sbom_dependencies,
)
from oss_ai_stack_map.pipeline.discovery import normalize_graphql_repo
from oss_ai_stack_map.pipeline.normalize import build_repo_technology_edges, build_technology_rows


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
            "defaultBranchRef": {"name": "main"},
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
    assert repo.default_branch == "main"
    assert repo.topics == ["llm", "agents"]
    assert repo.discovery_queries == ["topic:llm"]


def test_parse_manifest_dependencies_matches_registry_package_prefixes() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["ai-sdk"],
                    package_prefixes=["@ai-sdk/"],
                ),
                TechnologyAlias(
                    technology_id="steel-browser",
                    display_name="Steel Browser",
                    category_id="browser_and_computer_use_infra",
                    entity_type="product",
                    aliases=["steel-browser"],
                    package_prefixes=["@steel-browser/"],
                ),
                TechnologyAlias(
                    technology_id="e2b",
                    display_name="E2B",
                    category_id="sandbox_and_isolated_execution",
                    entity_type="product",
                    aliases=["e2b"],
                    package_prefixes=["e2b"],
                ),
                TechnologyAlias(
                    technology_id="daytona",
                    display_name="Daytona",
                    category_id="sandbox_and_isolated_execution",
                    entity_type="product",
                    aliases=["daytona"],
                    package_prefixes=["daytona"],
                ),
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    dependencies = parse_manifest_dependencies(
        "package.json",
        """
        {
          "dependencies": {
            "@ai-sdk/openai": "^3.0.0",
            "@steel-browser/api": "^0.5.1",
            "e2b-code-interpreter": "^1.0.0",
            "daytona": "^0.141.0"
          }
        }
        """,
        runtime.aliases.alias_lookup(),
        runtime.registry.alias_lookup(),
        runtime.registry.package_prefix_rules(),
    )

    by_package = {dependency.package_name: dependency for dependency in dependencies}

    assert by_package["@ai-sdk/openai"].technology_id == "vercel-ai-sdk"
    assert by_package["@ai-sdk/openai"].entity_type == "sdk_family"
    assert by_package["@ai-sdk/openai"].match_method == "package_prefix"
    assert by_package["@steel-browser/api"].technology_id == "steel-browser"
    assert by_package["e2b-code-interpreter"].technology_id == "e2b"
    assert by_package["daytona"].technology_id == "daytona"


def test_parse_sbom_dependencies_matches_registry_prefixes() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="browserbase",
                    display_name="Browserbase",
                    category_id="browser_and_computer_use_infra",
                    entity_type="product",
                    aliases=["browserbase"],
                    package_prefixes=["@browserbasehq/"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    dependencies = parse_sbom_dependencies(
        {
            "packages": [
                {
                    "SPDXID": "SPDXRef-root",
                    "name": "example-app",
                },
                {
                    "SPDXID": "SPDXRef-dep",
                    "name": "@browserbasehq/stagehand",
                    "versionInfo": "1.14.0",
                    "licenseConcluded": "MIT",
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": "pkg:npm/%40browserbasehq/stagehand@1.14.0",
                        }
                    ],
                },
            ],
            "relationships": [
                {
                    "relationshipType": "DESCRIBES",
                    "relatedSpdxElement": "SPDXRef-root",
                },
                {
                    "relationshipType": "DEPENDS_ON",
                    "spdxElementId": "SPDXRef-root",
                    "relatedSpdxElement": "SPDXRef-dep",
                },
            ],
        },
        runtime.aliases.alias_lookup(),
        runtime.registry.alias_lookup(),
        runtime.registry.package_prefix_rules(),
    )

    assert len(dependencies) == 1
    assert dependencies[0].technology_id == "browserbase"
    assert dependencies[0].match_method == "package_prefix"


def test_build_technology_rows_includes_registry_entries() -> None:
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
                    aliases=["openai"],
                )
            ]
        ),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["ai-sdk"],
                    package_prefixes=["@ai-sdk/"],
                    repo_names=["vercel/ai"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    rows = build_technology_rows(runtime)
    by_id = {row["technology_id"]: row for row in rows}

    assert "openai" in by_id
    assert by_id["vercel-ai-sdk"]["entity_type"] == "sdk_family"
    assert by_id["vercel-ai-sdk"]["repo_names"] == ["vercel/ai"]


def test_build_repo_technology_edges_adds_provider_subedge_for_family_match() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
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
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["ai-sdk"],
                    package_prefixes=["@ai-sdk/"],
                    repo_names=["vercel/ai"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    dependencies = parse_manifest_dependencies(
        "package.json",
        """
        {
          "dependencies": {
            "@ai-sdk/openai": "^3.0.0"
          }
        }
        """,
        runtime.aliases.alias_lookup(),
        runtime.registry.alias_lookup(),
        runtime.registry.package_prefix_rules(),
    )
    context = RepoContext(
        repo_id=1,
        full_name="example/app",
        manifest_dependencies=dependencies,
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="example/app",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])
    by_id = {edge.technology_id: edge for edge in edges}

    assert "vercel-ai-sdk" in by_id
    assert "openai" in by_id
    assert by_id["vercel-ai-sdk"].match_method == "package_prefix"
    assert by_id["openai"].match_method == "derived_provider"


def test_build_repo_technology_edges_adds_repo_identity_edge() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
        ),
        aliases=TechnologyAliasConfig(technologies=[]),
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="vercel-ai-sdk",
                    display_name="Vercel AI SDK",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["ai-sdk"],
                    repo_names=["vercel/ai"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    context = RepoContext(
        repo_id=1,
        full_name="vercel/ai",
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="vercel/ai",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])

    assert len(edges) == 1
    assert edges[0].technology_id == "vercel-ai-sdk"
    assert edges[0].evidence_type == "repo_identity"
    assert edges[0].evidence_path == "repo_metadata"
    assert edges[0].match_method == "repo_identity"


def test_parse_manifest_dependencies_matches_top_discovery_families() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
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
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="model-context-protocol",
                    display_name="Model Context Protocol",
                    category_id="ai_developer_tools_and_sdk_families",
                    entity_type="sdk_family",
                    aliases=["model-context-protocol", "mcp"],
                    package_prefixes=["@modelcontextprotocol/", "mcp"],
                ),
                TechnologyAlias(
                    technology_id="pytorch",
                    display_name="PyTorch",
                    category_id="training_finetuning_and_model_ops",
                    entity_type="product",
                    aliases=["pytorch", "torch"],
                    package_prefixes=["torch", "torchvision"],
                ),
                TechnologyAlias(
                    technology_id="huggingface-hub",
                    display_name="Hugging Face Hub",
                    category_id="training_finetuning_and_model_ops",
                    entity_type="product",
                    aliases=["huggingface-hub", "huggingface_hub"],
                    package_prefixes=["huggingface-hub", "huggingface_hub"],
                ),
                TechnologyAlias(
                    technology_id="tokenizers",
                    display_name="Tokenizers",
                    category_id="training_finetuning_and_model_ops",
                    entity_type="product",
                    aliases=["tokenizers"],
                    package_prefixes=["tokenizers"],
                ),
                TechnologyAlias(
                    technology_id="openai-agents",
                    display_name="OpenAI Agents",
                    category_id="orchestration_and_agents",
                    provider_id="openai",
                    entity_type="product",
                    aliases=["openai-agents", "@openai/agents"],
                    package_prefixes=["@openai/agents", "openai-agents"],
                ),
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )

    dependencies = parse_manifest_dependencies(
        "package.json",
        """
        {
          "dependencies": {
            "mcp": "^1.0.0",
            "@modelcontextprotocol/sdk": "^1.0.0",
            "torch": "^2.6.0",
            "torchvision": "^0.21.0",
            "huggingface-hub": "^0.30.0",
            "huggingface_hub": "^0.30.0",
            "tokenizers": "^0.21.0",
            "@openai/agents": "^0.0.8",
            "openai-agents": "^0.1.0"
          }
        }
        """,
        runtime.aliases.alias_lookup(),
        runtime.registry.alias_lookup(),
        runtime.registry.package_prefix_rules(),
    )

    by_package = {dependency.package_name: dependency for dependency in dependencies}

    assert by_package["mcp"].technology_id == "model-context-protocol"
    assert by_package["@modelcontextprotocol/sdk"].technology_id == "model-context-protocol"
    assert by_package["torch"].technology_id == "pytorch"
    assert by_package["torchvision"].technology_id == "pytorch"
    assert by_package["huggingface-hub"].technology_id == "huggingface-hub"
    assert by_package["huggingface_hub"].technology_id == "huggingface-hub"
    assert by_package["tokenizers"].technology_id == "tokenizers"
    assert by_package["@openai/agents"].technology_id == "openai-agents"
    assert by_package["@openai/agents"].provider_technology_id == "openai"
    assert by_package["openai-agents"].technology_id == "openai-agents"
    assert by_package["openai-agents"].provider_technology_id == "openai"


def test_build_repo_technology_edges_adds_provider_subedge_for_openai_agents() -> None:
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
            source_extensions=[".ts"],
            manifest_files=["package.json"],
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
        registry=TechnologyAliasConfig(
            technologies=[
                TechnologyAlias(
                    technology_id="openai-agents",
                    display_name="OpenAI Agents",
                    category_id="orchestration_and_agents",
                    provider_id="openai",
                    entity_type="product",
                    aliases=["openai-agents", "@openai/agents"],
                    package_prefixes=["@openai/agents", "openai-agents"],
                    repo_names=["openai/openai-agents-python"],
                )
            ]
        ),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    dependencies = parse_manifest_dependencies(
        "package.json",
        """
        {
          "dependencies": {
            "@openai/agents": "^0.0.8"
          }
        }
        """,
        runtime.aliases.alias_lookup(),
        runtime.registry.alias_lookup(),
        runtime.registry.package_prefix_rules(),
    )
    context = RepoContext(
        repo_id=1,
        full_name="example/app",
        manifest_dependencies=dependencies,
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="example/app",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])
    by_id = {edge.technology_id: edge for edge in edges}

    assert "openai-agents" in by_id
    assert "openai" in by_id
    assert by_id["openai-agents"].match_method == "registry_alias"
    assert by_id["openai"].match_method == "derived_provider"


def test_build_repo_technology_edges_can_fallback_to_readme_alias_mentions() -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            snapshot_date=date(2026, 3, 25),
            classification=ClassificationConfig(readme_mentions_used_for_edges=True),
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
                ),
                TechnologyAlias(
                    technology_id="anthropic",
                    display_name="Anthropic SDK",
                    category_id="model_access_and_providers",
                    provider_id="anthropic",
                    aliases=["anthropic"],
                ),
            ]
        ),
        registry=TechnologyAliasConfig(technologies=[]),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    context = RepoContext(
        repo_id=1,
        full_name="gateway/app",
        readme_text="Supports OpenAI and Anthropic APIs through a unified gateway.",
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="gateway/app",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])
    by_id = {edge.technology_id: edge for edge in edges}

    assert "openai" in by_id
    assert "anthropic" in by_id
    assert by_id["openai"].evidence_type == "readme_mention"
    assert by_id["openai"].match_method == "readme_alias"


def test_build_repo_technology_edges_can_fallback_to_punctuated_readme_alias_mentions() -> None:
    runtime = RuntimeConfig(
        config_dir=Path("config"),
        study=StudyConfig(
            snapshot_date=date(2026, 3, 25),
            classification=ClassificationConfig(readme_mentions_used_for_edges=True),
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
                    technology_id="llama-cpp",
                    display_name="llama.cpp",
                    category_id="serving_inference_and_local_runtimes",
                    aliases=["llama.cpp", "llama-cpp"],
                )
            ]
        ),
        registry=TechnologyAliasConfig(technologies=[]),
        segments=SegmentConfig(precedence=[], rules=[]),
        env=EnvSettings(github_token="test-token"),
    )
    context = RepoContext(
        repo_id=1,
        full_name="runanywhere/rcli",
        readme_text="M1 and M2 Macs fall back to llama.cpp automatically for local inference.",
    )
    decision = ClassificationDecision(
        repo_id=1,
        full_name="runanywhere/rcli",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
    )

    edges = build_repo_technology_edges(runtime=runtime, contexts=[context], decisions=[decision])

    assert len(edges) == 1
    assert edges[0].technology_id == "llama-cpp"
    assert edges[0].evidence_type == "readme_mention"
