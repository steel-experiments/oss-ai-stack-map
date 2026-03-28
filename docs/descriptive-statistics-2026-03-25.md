# Descriptive Statistics: 2026-03-25

This report summarizes `/home/agent/oss-ai-stack-map/data/run-2026-03-25-fresh-1mo-v1` from staged Parquet outputs.

| Metric | Count | Share of discovered repos |
| --- | --- | --- |
| Discovered repos | 1,216 | 100.0% |
| Serious repos | 963 | 79.2% |
| AI-relevant repos | 783 | 64.4% |
| Final repos | 783 | 64.4% |
| Judge-reviewed repos | 0 | 0.0% |

## Population profile

| Owner type | All repos | Final repos |
| --- | --- | --- |
| Organization | 859 (70.6%) | 617 (78.8%) |
| User | 357 (29.4%) | 166 (21.2%) |

| Rank | All repos | Final repos |
| --- | --- | --- |
| 1 | Python: 514 (42.3%) | Python: 397 (50.7%) |
| 2 | TypeScript: 240 (19.7%) | TypeScript: 176 (22.5%) |
| 3 | Go: 79 (6.5%) | Go: 49 (6.3%) |
| 4 | Rust: 63 (5.2%) | Rust: 35 (4.5%) |
| 5 | Unknown: 52 (4.3%) | Jupyter Notebook: 28 (3.6%) |
| 6 | JavaScript: 49 (4.0%) | JavaScript: 24 (3.1%) |
| 7 | Jupyter Notebook: 46 (3.8%) | C++: 17 (2.2%) |
| 8 | C++: 28 (2.3%) | Java: 14 (1.8%) |
| 9 | Java: 28 (2.3%) | Kotlin: 8 (1.0%) |
| 10 | HTML: 18 (1.5%) | C#: 7 (0.9%) |

Top licenses in the final set:

- `Apache-2.0`: `322`
- `MIT`: `271`
- `NOASSERTION`: `109`
- `AGPL-3.0`: `40`
- `GPL-3.0`: `12`

## Scores and evidence

- Serious score modes: `8` (290), `7` (256), `9` (220), `6` (124), `0` (124)
- AI score modes: `1` (175), `2` (144), `4` (88), `15` (73), `3` (73)
- Repos with manifest paths: `990` (81.4%)
- Repos with SBOM dependencies: `921` (75.7%)
- Repos with import-derived dependencies: `53` (4.4%)
- Evidence rows mapped to a known technology: `14116` (3.8%)

## Technology graph inputs

| Metric | Value |
| --- | --- |
| Normalized technology edges | 4,341 |
| Technologies in catalog | 63 |
| Final repos with at least one tracked edge | 778 |
| Final repos with no tracked edge | 5 |
| Missing tracked edges with only unmapped evidence | 5 |
| Missing tracked edges with no dependency evidence | 0 |

Top technologies in final repos:

| Rank | Technology | Repos | Share of final repos |
| --- | --- | --- | --- |
| 1 | OpenAI SDK | 456 | 58.2% |
| 2 | Model Context Protocol | 376 | 48.0% |
| 3 | Transformers | 278 | 35.5% |
| 4 | PyTorch | 264 | 33.7% |
| 5 | Anthropic SDK | 259 | 33.1% |
| 6 | Hugging Face Hub | 250 | 31.9% |
| 7 | Tokenizers | 220 | 28.1% |
| 8 | Google GenAI SDK | 192 | 24.5% |
| 9 | LangChain | 191 | 24.4% |
| 10 | Accelerate | 151 | 19.3% |

Edge category mix:

- `training_finetuning_and_model_ops`: `1309` (30.2%)
- `model_access_and_providers`: `1022` (23.5%)
- `orchestration_and_agents`: `699` (16.1%)
- `ai_developer_tools_and_sdk_families`: `467` (10.8%)
- `vector_and_knowledge_storage`: `309` (7.1%)
- `serving_inference_and_local_runtimes`: `223` (5.1%)
- `ui_and_app_frameworks`: `145` (3.3%)
- `observability_tracing_and_monitoring`: `65` (1.5%)
- `sandbox_and_isolated_execution`: `38` (0.9%)
- `browser_and_computer_use_infra`: `33` (0.8%)
- `evaluation_guardrails_and_safety`: `29` (0.7%)
- `runtime_and_agent_deployment`: `2` (0.0%)

Final primary segment mix:

- `serving_runtime`: `256` (32.7%)
- `training_finetuning`: `217` (27.7%)
- `orchestration_framework`: `99` (12.6%)
- `vector_retrieval_infrastructure`: `76` (9.7%)
- `agent_application`: `61` (7.8%)
- `ai_developer_tool`: `34` (4.3%)
- `rag_search_application`: `17` (2.2%)
- `unassigned`: `9` (1.1%)
- `eval_guardrails_observability`: `8` (1.0%)
- `ai_application`: `6` (0.8%)

Final repo stars summary:

- min `1,011`, median `4,995`, p75 `13,503`, max `338,092`

Final repo contexts:

- Final repos with manifest paths: `776` (99.1%)
- Final repos with SBOM dependencies: `664` (84.8%)
