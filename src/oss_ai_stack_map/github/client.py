from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.storage.cache import CacheStore


class GitHubClient:
    def __init__(self, runtime: RuntimeConfig) -> None:
        if not runtime.env.github_token:
            raise ValueError("GITHUB_TOKEN is required for GitHub API commands")
        self.runtime = runtime
        self.cache = CacheStore(
            Path("data/raw/github_cache") / runtime.study.snapshot_date.isoformat()
        )
        self.client = httpx.Client(
            base_url="https://api.github.com",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {runtime.env.github_token}",
                "User-Agent": runtime.study.http.user_agent,
                "X-GitHub-Api-Version": "2022-11-28",
            },
            follow_redirects=True,
            timeout=runtime.study.http.timeout_seconds,
        )

    def __enter__(self) -> "GitHubClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.client.close()

    def get_rate_limit(self) -> dict[str, Any]:
        return self._get_json("/rate_limit", cache_key="rate_limit", use_cache=False)

    def search_repositories(self, query: str, page: int, per_page: int = 100) -> dict[str, Any]:
        cache_key = f"search:{query}:page:{page}:per_page:{per_page}"
        return self._get_json(
            "/search/repositories",
            params={
                "q": query,
                "sort": "stars",
                "order": "desc",
                "page": page,
                "per_page": per_page,
            },
            cache_key=cache_key,
        )

    def get_repo_topics(self, owner: str, repo: str) -> list[str]:
        payload = self._get_json(
            f"/repos/{owner}/{repo}/topics",
            cache_key=f"topics:{owner}/{repo}",
            headers={"Accept": "application/vnd.github+json"},
        )
        return payload.get("names", [])

    def get_repositories_metadata(self, full_names: list[str]) -> dict[str, dict[str, Any]]:
        if not full_names:
            return {}

        cache_key = "repo-batch:v2:" + json.dumps(sorted(full_names))
        cached = self.cache.get_json("graphql", cache_key)
        if cached is not None:
            return cached

        query_parts = ["query RepoBatch {"]
        alias_to_name: dict[str, str] = {}
        for index, full_name in enumerate(full_names):
            owner, repo = full_name.split("/", 1)
            alias = f"repo_{index}"
            alias_to_name[alias] = full_name
            query_parts.append(
                f"""
  {alias}: repository(owner: "{owner}", name: "{repo}") {{
    databaseId
    nameWithOwner
    url
    description
    defaultBranchRef {{
      name
    }}
    isFork
    isArchived
    isTemplate
    stargazerCount
    forkCount
    createdAt
    updatedAt
    pushedAt
    owner {{
      __typename
      login
    }}
    primaryLanguage {{
      name
    }}
    licenseInfo {{
      spdxId
    }}
    repositoryTopics(first: 30) {{
      nodes {{
        topic {{
          name
        }}
      }}
    }}
  }}
"""
            )
        query_parts.append("}")
        payload = self._post_graphql("\n".join(query_parts), cache_key=cache_key)

        hydrated: dict[str, dict[str, Any]] = {}
        for alias, full_name in alias_to_name.items():
            repo_payload = payload.get("data", {}).get(alias)
            if repo_payload is not None:
                hydrated[full_name] = repo_payload
        self.cache.set_json("graphql", cache_key, hydrated)
        return hydrated

    def get_readme(self, owner: str, repo: str) -> str:
        payload = self._get_json(
            f"/repos/{owner}/{repo}/readme",
            cache_key=f"readme:{owner}/{repo}",
        )
        content = payload.get("content")
        if not content:
            return ""
        return base64.b64decode(content).decode("utf-8", errors="ignore")

    def get_tree(self, owner: str, repo: str, branch: str | None = None) -> list[str]:
        sha = branch
        if sha is None:
            repo_payload = self.get_repo(owner, repo)
            sha = repo_payload.get("default_branch", "HEAD")
        payload = self._get_json(
            f"/repos/{owner}/{repo}/git/trees/{sha}",
            params={"recursive": "1"},
            cache_key=f"tree:{owner}/{repo}:{sha}",
        )
        return [node["path"] for node in payload.get("tree", []) if node.get("type") == "blob"]

    def get_repo(self, owner: str, repo: str) -> dict[str, Any]:
        return self._get_json(f"/repos/{owner}/{repo}", cache_key=f"repo:{owner}/{repo}")

    def get_file_text(self, owner: str, repo: str, path: str) -> str:
        payload = self._get_json(
            f"/repos/{owner}/{repo}/contents/{path}",
            cache_key=f"file:{owner}/{repo}:{path}",
        )
        content = payload.get("content")
        if not content:
            return ""
        return base64.b64decode(content).decode("utf-8", errors="ignore")

    def get_sbom(self, owner: str, repo: str) -> dict[str, Any]:
        payload = self._get_json(
            f"/repos/{owner}/{repo}/dependency-graph/sbom",
            cache_key=f"sbom:{owner}/{repo}",
        )
        return payload.get("sbom", {})

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

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        cache_key: str,
        headers: dict[str, str] | None = None,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        if use_cache:
            cached = self.cache.get_json("rest", cache_key)
            if cached is not None:
                return cached
        response = self._request("GET", url, params=params, headers=headers)
        payload = response.json()
        if use_cache:
            self.cache.set_json("rest", cache_key, payload)
        return payload

    def _post_graphql(self, query: str, *, cache_key: str) -> dict[str, Any]:
        cached = self.cache.get_json("graphql-raw", cache_key)
        if cached is not None:
            return cached
        response = self._request("POST", "/graphql", json={"query": query})
        payload = response.json()
        self.cache.set_json("graphql-raw", cache_key, payload)
        return payload
