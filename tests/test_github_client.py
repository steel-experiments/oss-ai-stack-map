from __future__ import annotations

import httpx

from oss_ai_stack_map.github.client import GitHubClient


def test_get_rate_limit_bypasses_cache(runtime_config, monkeypatch) -> None:
    runtime = runtime_config["runtime"]
    with GitHubClient(runtime=runtime) as client:
        monkeypatch.setattr(
            client.cache,
            "get_json",
            lambda namespace, key: {"resources": {"core": {"remaining": 0}}},
        )

        requested: list[tuple[str, str]] = []

        def fake_request(method: str, url: str, **kwargs):
            requested.append((method, url))
            request = httpx.Request(method, f"https://api.github.com{url}")
            return httpx.Response(
                200,
                request=request,
                json={"resources": {"core": {"remaining": 4999}}},
            )

        monkeypatch.setattr(client, "_request", fake_request)

        payload = client.get_rate_limit()

    assert requested == [("GET", "/rate_limit")]
    assert payload["resources"]["core"]["remaining"] == 4999
