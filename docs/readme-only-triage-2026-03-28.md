# README-Only Final Repo Triage

Date: `2026-03-28`

Snapshot reviewed: [data/run-2026-03-25-repaired-v16](/home/agent/oss-ai-stack-map/data/run-2026-03-25-repaired-v16)

## Summary

The validated `v16` snapshot contains `41` final repos whose normalized technology edges come only from low-confidence README mentions.

That is down from `55` in `v15`, mainly because new canonical repo-identity coverage converted part of the old README-only queue into stronger mapped edges.

These `41` repos remain the active review queue for the project’s reviewed README fallback policy.

## Review policy

- keep README-only repos visible as low-confidence fallback coverage when they would otherwise remain unmapped
- do not interpret README-only edges the same way as manifest, SBOM, import, or repo-identity evidence
- prioritize direct identity or package-prefix fixes before relying on README-only graph presence
- review judge-overridden README-only repos first, because they are the highest-risk queue for public graph claims

## Current shortlist

| Repo | Stars | Segment | Judge override | README-only technologies |
| --- | --- | --- | --- | --- |
| `Kong/kong` | 43,037 | `serving_runtime` | yes | `openai`, `anthropic` |
| `farion1231/cc-switch` | 33,306 | `agent_application` | yes | `openai` |
| `songquanpeng/one-api` | 30,975 | `serving_runtime` | yes | `openai`, `anthropic`, `ollama`, `cloudflare-agents` |
| `SillyTavern/SillyTavern` | 24,811 | `serving_runtime` | no | `openai` |
| `vanna-ai/vanna` | 23,107 | `vector_retrieval_infrastructure` | no | `openai`, `anthropic`, `ollama` |
| `RightNow-AI/openfang` | 15,518 | `serving_runtime` | no | `langchain`, `langgraph`, `autogen`, `crewai`, `guardrails`, `anthropic` |
| `rtk-ai/rtk` | 13,186 | `serving_runtime` | no | `openai` |
| `cft0808/edict` | 12,733 | `serving_runtime` | no | `autogen`, `crewai`, `langgraph` |
| `oramasearch/orama` | 10,260 | `serving_runtime` | yes | `openai` |
| `sigoden/aichat` | 9,617 | `rag_search_application` | yes | `openai`, `ollama` |
| `BlockRunAI/ClawRouter` | 6,390 | `serving_runtime` | no | `openai`, `anthropic`, `litellm` |
| `olimorris/codecompanion.nvim` | 6,344 | `serving_runtime` | yes | `openai`, `anthropic`, `ollama`, `model-context-protocol` |

## What changed in v16

- `weaviate/weaviate` left the README-only queue after canonical repo-identity support was added
- several benchmarked upstream repos now map through stronger repo-identity edges instead of README-only fallback
- the remaining queue is more concentrated in app-layer repos and judge-overridden border cases than in obvious registry misses

## Next passes

- review the judge-overridden rows first
- add more canonical repo identities where the project itself is the technology being measured
- add package-prefix coverage where README-only usage appears to be compensating for a known mapping gap
