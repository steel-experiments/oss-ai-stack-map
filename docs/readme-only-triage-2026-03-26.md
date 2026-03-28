# README-Only Final Repo Triage

Date: `2026-03-26`

Snapshot reviewed: [data/run-2026-03-25-repaired-v15](/home/agent/oss-ai-stack-map/data/run-2026-03-25-repaired-v15)

## Summary

The validated `v15` snapshot contains `55` final repos whose normalized technology edges come only from low-confidence README mentions.

That population splits into three queues:

- `19` borderline repos included only after judge override
- `7` likely identity or registry mapping misses
- `29` repos that may belong in the final set, but should not be treated as strong graph evidence until direct mapped dependencies or canonical identity mappings exist

## Priority Order

1. Fix identity and registry misses first.
   These are the cheapest credibility wins because the repos appear to be real upstream products, but the graph currently knows them only through README mentions.
2. Review the judge-only queue second.
   These are the highest-risk false positives in graph-style claims because they are both README-only and rule-negative.
3. Keep the remaining README-only repos in the final set only if the project is comfortable distinguishing final-set membership from graph-evidence strength.

## Queue A: Identity Or Registry Gaps

These look like repos the map should recognize more directly than it currently does.

| Repo | Stars | Current segment | README-only technologies | Proposed action |
| --- | --- | --- | --- | --- |
| `weaviate/weaviate` | 15,879 | `vector_retrieval_infrastructure` | `weaviate`, `langchain`, `openai`, `haystack`, `semantic-kernel`, more | Add canonical repo identity mapping for Weaviate and review whether README-only companion integrations should remain as edges. |
| `vanna-ai/vanna` | 23,107 | `vector_retrieval_infrastructure` | `openai`, `anthropic`, `ollama` | Check whether Vanna belongs in the registry as a product identity and whether repo-level identity edges are appropriate. |
| `langchain4j/langchain4j` | 11,287 | `vector_retrieval_infrastructure` | `langchain`, `milvus`, `openai`, `haystack` | Add or review canonical repo identity support for LangChain4j rather than inferring it only through README mentions. |
| `langgraph4j/langgraph4j` | 1,466 | `agent_application` | `langgraph`, `langchain`, `langfuse`, `openai` | Same issue as LangChain4j: likely deserves direct repo identity handling. |
| `vearch/vearch` | 2,291 | `serving_runtime` | `langchain`, `llamaindex` | Review whether Vearch should map through repo identity or package-prefix coverage. |
| `xtreme1-io/xtreme1` | 1,171 | `serving_runtime` | `accelerate` | Check whether the product itself belongs in the registry or whether this is a weak README-only false signal. |
| `tursodatabase/agentfs` | 2,727 | `agent_application` | `mastra`, `vercel-ai-sdk`, `openai-agents`, `openai`, `anthropic` | Review whether this is a registry-gap product or a final-set app that should stay outside strong graph claims. |

## Queue B: Borderline Judge-Only Repos

These repos were rule-negative for final inclusion and entered the final set only through judge override, while still remaining README-only in the graph.

| Repo | Stars | Score AI | Segment | README-only technologies |
| --- | --- | --- | --- | --- |
| `Kong/kong` | 43,037 | 2 | `serving_runtime` | `openai`, `anthropic` |
| `farion1231/cc-switch` | 33,306 | 2 | `agent_application` | `openai` |
| `songquanpeng/one-api` | 30,975 | 2 | `serving_runtime` | `openai`, `anthropic`, `ollama`, `cloudflare-agents` |
| `charmbracelet/crush` | 21,953 | 2 | `serving_runtime` | `openai`, `anthropic`, `ollama` |
| `oramasearch/orama` | 10,260 | 2 | `serving_runtime` | `openai` |
| `sigoden/aichat` | 9,617 | 2 | `rag_search_application` | `openai`, `ollama` |
| `gorse-io/gorse` | 9,574 | 2 | `vector_retrieval_infrastructure` | `transformers`, `vllm`, `ollama` |
| `olimorris/codecompanion.nvim` | 6,344 | 2 | `serving_runtime` | `openai`, `anthropic`, `ollama`, `model-context-protocol` |
| `google-deepmind/acme` | 3,947 | 2 | `agent_application` | `openai` |
| `Lightning-AI/LitServe` | 3,833 | 2 | `serving_runtime` | `pytorch`, `vllm`, `openai`, `ollama` |
| `max-sixty/worktrunk` | 3,710 | 2 | `agent_application` | `anthropic` |
| `karthink/gptel` | 3,265 | 2 | `ai_developer_tool` | `openai`, `anthropic`, `ollama`, `model-context-protocol` |
| `vearch/vearch` | 2,291 | 2 | `serving_runtime` | `langchain`, `llamaindex` |
| `korotovsky/slack-mcp-server` | 1,477 | 2 | `serving_runtime` | `model-context-protocol` |
| `LLPhant/LLPhant` | 1,445 | 2 | `vector_retrieval_infrastructure` | `openai`, `anthropic`, `ollama`, `langchain` |
| `sauravpanda/BrowserAI` | 1,383 | 2 | `orchestration_framework` | `transformers` |
| `ComposioHQ/secure-openclaw` | 1,333 | 2 | `ai_application` | `anthropic` |
| `valentinfrlch/ha-llmvision` | 1,282 | 1 | `unassigned` | `openai`, `anthropic`, `ollama` |
| `stakpak/agent` | 1,183 | 2 | `agent_application` | `openai`, `anthropic`, `ollama`, `model-context-protocol`, `guardrails` |

Recommended policy for Queue B:

- keep them in the final set only if the project explicitly allows judged, README-evidence-only repos
- exclude them from graph-centrality, co-occurrence, and community claims until stronger mapped evidence exists

## Queue C: Retain But Not Graph-Strong

These repos were already rule-positive, but their current graph presence is still only README-derived. They are lower-risk than Queue B for inclusion, but still weak evidence for stack-shape claims.

Highest-priority examples:

- `SillyTavern/SillyTavern`
- `coze-dev/coze-studio`
- `RightNow-AI/openfang`
- `plandex-ai/plandex`
- `rtk-ai/rtk`
- `BlockRunAI/ClawRouter`
- `casibase/casibase`
- `JetBrains/koog`
- `embabel/embabel-agent`
- `langwatch/better-agents`

Recommended policy for Queue C:

- keep them in the final repo population if the repo-level inclusion methodology supports that
- do not let README-only edges carry the same interpretive weight as manifest, SBOM, import, or repo-identity edges
- add direct mappings where possible, especially for canonical upstream repos and strongly branded SDK/framework repos

## Publication Guidance

For future public reports, the safest wording is:

- final repo population includes serious AI repos selected by rules plus limited judge review
- graph analysis describes the technology-connected subset of final repos
- README-only edges are fallback coverage, not strong dependency evidence

That keeps the final-set question separate from the stack-graph question, which is the right methodological boundary for this project.
