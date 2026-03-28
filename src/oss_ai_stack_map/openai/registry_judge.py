from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.storage.cache import CacheStore


class OpenAIRegistryJudge:
    def __init__(self, runtime: RuntimeConfig) -> None:
        self.runtime = runtime
        self.cache = CacheStore(
            Path("data/raw/openai_registry_judge") / runtime.study.snapshot_date.isoformat()
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

    def __enter__(self) -> "OpenAIRegistryJudge":
        return self

    def __exit__(self, *_: object) -> None:
        self.client.close()

    def review_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        cache_key = sha256(json.dumps(candidate, sort_keys=True).encode("utf-8")).hexdigest()
        cached = self.cache.get_json("registry_candidate", cache_key)
        if cached is not None:
            return cached

        payload = {
            "model": self.runtime.study.judge.model,
            "input": [
                {
                    "role": "system",
                    "content": registry_judge_system_prompt(),
                },
                {
                    "role": "user",
                    "content": json.dumps(candidate, indent=2),
                },
            ],
            "tools": [registry_judge_tool_definition()],
            "tool_choice": {
                "type": "function",
                "name": "submit_registry_candidate_judgment",
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
        self.cache.set_json("registry_candidate", cache_key, arguments)
        return arguments

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


def registry_judge_system_prompt() -> str:
    return (
        "You are a conservative registry curator for an OSS AI stack map. "
        "Your job is to review data-derived candidate technology families before they are promoted "
        "into the canonical technology registry. Favor precision over recall. Reject generic UI, "
        "frontend, testing, devtool, language, styling, and infrastructure families unless the "
        "evidence clearly shows a distinct AI-stack technology family. "
        "If accepted, normalize the candidate into a stable canonical entity id, display name, "
        "category, aliases, package prefixes, and canonical repo names. "
        "If the candidate is plausible but not strong enough, choose review instead of accept."
    )


def registry_judge_tool_definition() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "submit_registry_candidate_judgment",
        "description": "Return a conservative judgment for a canonical registry candidate.",
        "parameters": {
            "type": "object",
            "properties": {
                "decision": {
                    "type": "string",
                    "enum": ["accept", "review", "reject"],
                },
                "confidence": {
                    "type": "string",
                    "enum": ["low", "medium", "high"],
                },
                "canonical_entity_id": {
                    "type": ["string", "null"],
                },
                "canonical_display_name": {
                    "type": ["string", "null"],
                },
                "category_id": {
                    "type": ["string", "null"],
                },
                "aliases": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "package_prefixes": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "repo_names": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "reasons": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [
                "decision",
                "confidence",
                "canonical_entity_id",
                "canonical_display_name",
                "category_id",
                "aliases",
                "package_prefixes",
                "repo_names",
                "reasons",
            ],
            "additionalProperties": False,
        },
        "strict": True,
    }
