from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.models.core import (
    ClassificationDecision,
    DiscoveredRepo,
    JudgeDecision,
    RepoContext,
)
from oss_ai_stack_map.storage.cache import CacheStore


class OpenAIJudge:
    def __init__(self, runtime: RuntimeConfig) -> None:
        self.runtime = runtime
        self.cache = CacheStore(
            Path("data/raw/openai_judge") / runtime.study.snapshot_date.isoformat()
        )
        self.client = httpx.Client(
            base_url="https://api.openai.com/v1",
            headers={
                "Authorization": f"Bearer {runtime.env.openai_api_key}",
                "Content-Type": "application/json",
                "User-Agent": runtime.study.http.user_agent,
            },
            timeout=runtime.study.http.timeout_seconds,
        )

    def __enter__(self) -> "OpenAIJudge":
        return self

    def __exit__(self, *_: object) -> None:
        self.client.close()

    def judge_repo(
        self,
        repo: DiscoveredRepo,
        context: RepoContext,
        decision: ClassificationDecision,
        judge_mode: str = "hardening",
    ) -> JudgeDecision:
        evidence = build_evidence_packet(
            repo=repo,
            context=context,
            decision=decision,
            judge_mode=judge_mode,
        )
        cache_key = sha256(json.dumps(evidence, sort_keys=True).encode("utf-8")).hexdigest()
        cached = self.cache.get_json(f"judge_{judge_mode}", cache_key)
        if cached is not None:
            return JudgeDecision.model_validate(cached)

        payload = {
            "model": self.runtime.study.judge.model,
            "input": [
                {
                    "role": "system",
                    "content": system_prompt_for_mode(judge_mode),
                },
                {
                    "role": "user",
                    "content": json.dumps(evidence, indent=2),
                },
            ],
            "tools": [judge_tool_definition()],
            "tool_choice": {
                "type": "function",
                "name": "submit_repo_judgment",
            },
            "parallel_tool_calls": False,
            "reasoning": {
                "effort": self.runtime.study.judge.reasoning_effort,
            },
        }
        response = self._request("POST", "/responses", json=payload)
        response_payload = response.json()
        function_call = next(
            item
            for item in response_payload.get("output", [])
            if item.get("type") == "function_call"
        )
        arguments = json.loads(function_call["arguments"])
        judgment = JudgeDecision(
            repo_id=repo.repo_id,
            full_name=repo.full_name,
            judge_mode=judge_mode,
            serious_project=arguments["serious_project"],
            ai_relevant=arguments["ai_relevant"],
            include_in_final_set=arguments["include_in_final_set"],
            primary_segment=arguments["primary_segment"],
            confidence=arguments["confidence"],
            override_rule_decision=arguments["override_rule_decision"],
            reasons=arguments["reasons"],
            model=self.runtime.study.judge.model,
        )
        self.cache.set_json(f"judge_{judge_mode}", cache_key, judgment.to_row())
        return judgment

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=20),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        response = self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response


def build_evidence_packet(
    repo: DiscoveredRepo,
    context: RepoContext,
    decision: ClassificationDecision,
    judge_mode: str,
) -> dict[str, Any]:
    manifest_techs = sorted(
        {dep.technology_id for dep in context.manifest_dependencies if dep.technology_id}
    )
    sbom_techs = sorted(
        {dep.technology_id for dep in context.sbom_dependencies if dep.technology_id}
    )
    import_techs = sorted(
        {dep.technology_id for dep in context.import_dependencies if dep.technology_id}
    )
    return {
        "judge_mode": judge_mode,
        "repo": {
            "full_name": repo.full_name,
            "description": repo.description,
            "stars": repo.stars,
            "primary_language": repo.primary_language,
            "topics": repo.topics,
        },
        "rule_decision": {
            "passed_serious_filter": decision.passed_serious_filter,
            "passed_ai_relevance_filter": decision.passed_ai_relevance_filter,
            "passed_major_filter": decision.passed_major_filter,
            "score_serious": decision.score_serious,
            "score_ai": decision.score_ai,
            "exclusion_reason": decision.exclusion_reason,
            "primary_segment": decision.primary_segment,
            "notes": decision.notes[:20],
        },
        "evidence": {
            "manifest_paths": context.manifest_paths[:20],
            "manifest_technologies": manifest_techs,
            "sbom_technologies": sbom_techs,
            "import_technologies": import_techs,
            "tree_path_sample": context.tree_paths[:50],
            "readme_excerpt": context.readme_text[:2500],
        },
    }


def system_prompt_for_mode(judge_mode: str) -> str:
    base = (
        "You are a conservative repository classifier. "
        "Judge only whether the repository should be included in a "
        "current-state map of major open source AI projects. Favor "
        "precision over recall. Exclude educational, book, tutorial, "
        "prompt-list, and non-product repos. "
    )
    if judge_mode == "validation":
        return (
            base
            + "This repo is currently included in the final selected set. "
            "Treat this as a false-positive validation pass and remove the repo "
            "when the evidence is weak or primarily educational, list-like, or "
            "non-product. Only keep inclusion when the evidence clearly supports it. "
            "If your inclusion judgment differs from the current rule-based final-set "
            "decision, set override_rule_decision to true."
        )
    return base + "Only override the rule-based decision when the evidence clearly supports it."


def judge_tool_definition() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "submit_repo_judgment",
        "description": (
            "Return a conservative repository judgment for inclusion in the final "
            "major AI repo set. "
            "Use only the provided evidence."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "serious_project": {
                    "type": "boolean",
                    "description": (
                        "Whether this repository is a serious product/tooling repo "
                        "rather than an educational or list repo."
                    ),
                },
                "ai_relevant": {
                    "type": "boolean",
                    "description": (
                        "Whether the repository's primary product or workflow "
                        "meaningfully depends on AI/ML/LLM capabilities."
                    ),
                },
                "include_in_final_set": {
                    "type": "boolean",
                    "description": (
                        "Whether the repo should be included in the final major AI "
                        "repo set."
                    ),
                },
                "primary_segment": {
                    "type": ["string", "null"],
                    "description": "Best primary segment, or null if unclear.",
                },
                "confidence": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                    "description": "Confidence in the judgment.",
                },
                "override_rule_decision": {
                    "type": "boolean",
                    "description": (
                        "Whether the model believes the rule-based decision should "
                        "be overridden. In validation mode, set this to true when "
                        "include_in_final_set differs from the current final-set decision."
                    ),
                },
                "reasons": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Short reasons grounded in the evidence.",
                },
            },
            "required": [
                "serious_project",
                "ai_relevant",
                "include_in_final_set",
                "primary_segment",
                "confidence",
                "override_rule_decision",
                "reasons",
            ],
            "additionalProperties": False,
        },
        "strict": True,
    }
