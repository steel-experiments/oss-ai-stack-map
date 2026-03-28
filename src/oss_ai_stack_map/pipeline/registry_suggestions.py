from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.openai.registry_judge import OpenAIRegistryJudge
from oss_ai_stack_map.pipeline.technology_discovery import (
    build_technology_discovery_report,
    generic_family_penalty,
    infer_candidate_family_id,
    package_has_ai_affinity,
)

GENERIC_REGISTRY_FAMILY_IDS = {
    "agent",
    "agents",
    "apollo",
    "browser",
    "case",
    "chat",
    "client",
    "clients",
    "core",
    "default",
    "dev",
    "federated",
    "global",
    "http",
    "ibm",
    "logger",
    "model",
    "oauth",
    "oauth2",
    "open",
    "otel",
    "platform",
    "prompt",
    "prompts",
    "ruby",
    "sdk",
    "server",
    "service",
    "services",
    "tool",
    "tools",
    "ui",
    "web",
    "worker",
    "workers",
    "workflow",
    "workflows",
}
GENERIC_REGISTRY_SCOPES = {
    "@angular",
    "@apollo",
    "@astrojs",
    "@babel",
    "@fastify",
    "@hono",
    "@img",
    "@metorial-mcp-containers",
    "@nestjs",
    "@node-oauth",
    "@radix-ui",
    "@rollup",
    "@shikijs",
    "@solidjs",
    "@types",
}
ABSTRACT_CANONICAL_FAMILIES = {
    "diffusion",
    "prompt",
    "prompts",
    "rag",
    "workflow",
    "workflows",
}


@dataclass
class RegistrySuggestionReport:
    suggestion_count: int
    llm_reviewed_count: int
    suggestions: list[dict[str, Any]]


def build_registry_suggestion_report(
    *,
    input_dir: Path,
    runtime: RuntimeConfig,
    top_n: int = 25,
    judge_with_llm: bool = False,
) -> RegistrySuggestionReport:
    discovery_path = input_dir / "technology_discovery_report.json"
    if discovery_path.exists():
        discovery_payload = json.loads(discovery_path.read_text(encoding="utf-8"))
    else:
        discovery_payload = build_technology_discovery_report(
            input_dir=input_dir,
            runtime=runtime,
            top_n=top_n,
        ).__dict__

    known_tokens = build_known_family_tokens(runtime)
    llm_reviewed_count = 0
    suggestions: list[dict[str, Any]] = []

    judge = None
    if judge_with_llm and runtime.env.openai_api_key:
        judge = OpenAIRegistryJudge(runtime)
        judge.__enter__()
    try:
        for row in discovery_payload.get("top_candidates", [])[:top_n]:
            family_id = row["family_id"]
            if family_id in known_tokens:
                continue
            if not should_include_registry_suggestion(row):
                continue
            package_prefixes = suggest_package_prefixes(row)
            repo_names = suggest_repo_names(row)
            if not package_prefixes and not repo_names:
                continue
            suggested_entity_id, suggested_display_name = canonicalize_suggestion_identity(
                row=row,
                package_prefixes=package_prefixes,
                repo_names=repo_names,
            )
            suggestion = {
                "candidate_family_id": family_id,
                "suggested_entity_id": suggested_entity_id,
                "suggested_display_name": suggested_display_name,
                "suggested_category_id": suggest_category_id(row),
                "suggested_aliases": suggest_aliases(row),
                "suggested_package_prefixes": package_prefixes,
                "suggested_repo_names": repo_names,
                "priority_score": row["priority_score"],
                "confidence": suggestion_confidence(row),
                "final_repo_count": row["final_repo_count"],
                "missing_edge_repo_count": row["missing_edge_repo_count"],
                "benchmark_overlap_count": row.get("benchmark_overlap_count", 0),
                "example_packages": row.get("example_packages", [])[:5],
                "example_repos": row.get("example_repos", [])[:5],
            }
            if judge is not None and should_send_to_llm_judge(row, suggestion):
                suggestion["llm_review"] = judge.review_candidate(suggestion)
                llm_reviewed_count += 1
            suggestions.append(suggestion)
    finally:
        if judge is not None:
            judge.__exit__()

    return RegistrySuggestionReport(
        suggestion_count=len(suggestions),
        llm_reviewed_count=llm_reviewed_count,
        suggestions=suggestions,
    )


def build_known_family_tokens(runtime: RuntimeConfig) -> set[str]:
    tokens: set[str] = set()
    for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]:
        tokens.add(infer_candidate_family_id(technology.technology_id))
        for alias in technology.aliases:
            tokens.add(infer_candidate_family_id(alias))
        for prefix in technology.package_prefixes:
            tokens.add(infer_candidate_family_id(prefix))
        for repo_name in technology.repo_names:
            _, _, repo = repo_name.partition("/")
            if repo:
                tokens.add(infer_candidate_family_id(repo))
    return {token for token in tokens if token}


def suggest_aliases(row: dict[str, Any]) -> list[str]:
    family_id = row.get("candidate_family_id") or row.get("family_id") or ""
    aliases = [family_id]
    for package_name in row.get("example_packages", []):
        normalized = package_name.casefold()
        if normalized.startswith("@"):
            aliases.append(package_name)
        elif normalized == family_id:
            aliases.append(package_name)
    return dedupe_preserve_order(alias for alias in aliases if alias)


def canonicalize_suggestion_identity(
    *,
    row: dict[str, Any],
    package_prefixes: list[str],
    repo_names: list[str],
) -> tuple[str, str]:
    family_id = row.get("candidate_family_id") or row.get("family_id") or ""
    if family_id.casefold() in ABSTRACT_CANONICAL_FAMILIES:
        scoped_prefix = next((prefix for prefix in package_prefixes if "/" in prefix), None)
        if scoped_prefix:
            return slugify(scoped_prefix.lstrip("@")), display_name_from_anchor(scoped_prefix)
        repo_name = next((name for name in repo_names if "/" in name), None)
        if repo_name:
            return slugify(repo_name.replace("/", "-")), display_name_from_anchor(repo_name)
    return slugify(family_id), row["display_name"]


def suggest_package_prefixes(row: dict[str, Any]) -> list[str]:
    family_id = row.get("candidate_family_id") or row.get("family_id") or ""
    prefixes: list[str] = []
    for package_name in row.get("example_packages", []):
        normalized = package_name.casefold()
        if normalized.startswith("@") and "/" in normalized:
            scope, remainder = normalized.split("/", 1)
            if infer_candidate_family_id(package_name) == family_id:
                base = re.split(r"[-_.]", remainder, maxsplit=1)[0]
                prefixes.append(f"{scope}/{base}")
            continue
        if normalized == family_id:
            prefixes.append(package_name)
            continue
        if normalized.startswith(f"{family_id}-") or normalized.startswith(f"{family_id}_"):
            prefixes.append(family_id)
    if not prefixes and row.get("scoped_package_count", 0) == 0 and package_has_ai_affinity(family_id):
        prefixes.append(family_id)
    filtered = [
        prefix
        for prefix in dedupe_preserve_order(prefix for prefix in prefixes if prefix)
        if not is_generic_registry_prefix(prefix)
    ]
    return filtered[:5]


def suggest_repo_names(row: dict[str, Any]) -> list[str]:
    repo_names = []
    for repo_name in row.get("suggested_repo_names", [])[:5]:
        normalized = repo_name.casefold()
        if is_generic_registry_repo_name(normalized):
            continue
        repo_names.append(repo_name)
    return dedupe_preserve_order(repo_names)


def suggest_category_id(row: dict[str, Any]) -> str:
    family_id = row.get("candidate_family_id") or row.get("family_id") or ""
    text = " ".join(
        [
            family_id,
            *row.get("example_packages", []),
            *row.get("suggested_repo_names", []),
        ]
    ).casefold()
    if any(token in text for token in ("browser", "playwright", "stagehand", "chrome")):
        return "browser_and_computer_use_infra"
    if any(token in text for token in ("qdrant", "weaviate", "chroma", "milvus", "vector")):
        return "vector_and_knowledge_storage"
    if any(token in text for token in ("vllm", "ollama", "inference", "serve", "cuda")):
        return "serving_inference_and_local_runtimes"
    if any(token in text for token in ("trace", "observ", "logfire", "langfuse", "phoenix")):
        return "observability_tracing_and_monitoring"
    if any(token in text for token in ("evaluate", "eval", "guardrail", "safety")):
        return "evaluation_guardrails_and_safety"
    if any(
        token in text
        for token in (
            "torch",
            "transformers",
            "tokenizer",
            "huggingface",
            "dataset",
            "deepspeed",
            "accelerate",
        )
    ):
        return "training_finetuning_and_model_ops"
    if any(
        token in text
        for token in ("agent", "workflow", "prompt", "copilot", "assistant", "rag", "diffusion")
    ):
        return "orchestration_and_agents"
    if any(token in text for token in ("mcp", "sdk", "protocol")):
        return "ai_developer_tools_and_sdk_families"
    return "ai_developer_tools_and_sdk_families"


def suggestion_confidence(row: dict[str, Any]) -> str:
    score = float(row.get("priority_score", 0.0))
    ai_signal_count = candidate_ai_signal_count(row)
    if (
        score >= 35
        and ai_signal_count >= 2
        and (row.get("scoped_package_count", 0) > 0 or row.get("suggested_repo_names"))
    ):
        return "high"
    if score >= 20 and ai_signal_count >= 1:
        return "medium"
    return "low"


def should_include_registry_suggestion(row: dict[str, Any]) -> bool:
    family_id = (row.get("candidate_family_id") or row.get("family_id") or "").casefold()
    if not family_id:
        return False
    if is_generic_registry_family(family_id):
        return False
    if generic_family_penalty(family_id) >= 0.6 and int(row.get("benchmark_overlap_count", 0) or 0) == 0:
        return False

    ai_signal_count = candidate_ai_signal_count(row)
    benchmark_overlap_count = int(row.get("benchmark_overlap_count", 0) or 0)
    missing_edge_repo_count = int(row.get("missing_edge_repo_count", 0) or 0)
    final_repo_count = int(row.get("final_repo_count", 0) or 0)
    priority_score = float(row.get("priority_score", 0.0) or 0.0)
    package_prefixes = suggest_package_prefixes(row)
    repo_names = suggest_repo_names(row)
    ai_package_ratio = candidate_ai_package_ratio(row)
    family_is_abstract = family_id in ABSTRACT_CANONICAL_FAMILIES

    if ai_signal_count == 0 and benchmark_overlap_count == 0:
        return False
    if priority_score < 20 and benchmark_overlap_count == 0:
        return False
    if ai_signal_count < 2 and benchmark_overlap_count == 0 and missing_edge_repo_count == 0:
        return False
    if benchmark_overlap_count == 0 and not package_has_ai_affinity(family_id) and ai_package_ratio < 0.34:
        return False
    if not package_prefixes and not repo_names and benchmark_overlap_count == 0:
        return False
    if final_repo_count < 2 and missing_edge_repo_count == 0 and benchmark_overlap_count == 0:
        return False
    if family_is_abstract and not any("/" in value for value in [*package_prefixes, *repo_names]):
        return False
    return True


def should_send_to_llm_judge(row: dict[str, Any], suggestion: dict[str, Any]) -> bool:
    if suggestion.get("confidence") == "low":
        return False
    if not suggestion.get("suggested_package_prefixes") and not suggestion.get("suggested_repo_names"):
        return False
    return candidate_ai_signal_count(row) >= 2 or int(row.get("benchmark_overlap_count", 0) or 0) > 0


def candidate_ai_signal_count(row: dict[str, Any]) -> int:
    signals = 0
    if package_has_ai_affinity(row.get("candidate_family_id") or row.get("family_id") or ""):
        signals += 2
    values = [
        *row.get("example_packages", []),
        *row.get("suggested_repo_names", []),
    ]
    signals += sum(1 for value in values if package_has_ai_affinity(value))
    return signals


def candidate_ai_package_ratio(row: dict[str, Any]) -> float:
    packages = [package for package in row.get("example_packages", []) if package]
    if not packages:
        return 0.0
    ai_packages = sum(1 for package in packages if package_has_ai_affinity(package))
    return ai_packages / len(packages)


def is_generic_registry_family(family_id: str) -> bool:
    normalized = family_id.casefold()
    return normalized in GENERIC_REGISTRY_FAMILY_IDS


def is_generic_registry_prefix(prefix: str) -> bool:
    normalized = prefix.casefold()
    if normalized in GENERIC_REGISTRY_FAMILY_IDS:
        return True
    if normalized.startswith("@"):
        scope, _, remainder = normalized.partition("/")
        if scope in GENERIC_REGISTRY_SCOPES:
            return True
        base = infer_candidate_family_id(remainder or normalized)
        if base in GENERIC_REGISTRY_FAMILY_IDS:
            return True
    return False


def is_generic_registry_repo_name(repo_name: str) -> bool:
    owner, _, repo = repo_name.partition("/")
    if not owner or not repo:
        return True
    return is_generic_registry_family(infer_candidate_family_id(repo))


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug or value.casefold()


def display_name_from_anchor(value: str) -> str:
    normalized = value.lstrip("@")
    if "/" not in normalized:
        return normalized.title()
    owner, name = normalized.split("/", 1)
    return f"{titleish_token(owner)}/{titleish_token(name)}"


def titleish_token(value: str) -> str:
    token = value.replace("_", "-")
    parts = [part for part in token.split("-") if part]
    normalized_parts: list[str] = []
    for part in parts:
        if part in {"ai", "api", "llm", "mcp", "rag", "sdk", "ui"}:
            normalized_parts.append(part.upper())
        else:
            normalized_parts.append(part[:1].upper() + part[1:])
    return "-".join(normalized_parts) or value


def dedupe_preserve_order(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = value.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(value)
    return result
