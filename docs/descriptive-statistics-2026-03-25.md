# Descriptive Statistics: 2026-03-25

This report summarizes `/home/agent/oss-ai-stack-map/data/run-2026-03-25-resumable` from staged Parquet outputs.

| Metric | Count | Share of discovered repos |
| --- | --- | --- |
| Discovered repos | 1,522 | 100.0% |
| Serious repos | 1,257 | 82.6% |
| AI-relevant repos | 897 | 58.9% |
| Final repos | 897 | 58.9% |
| Judge-reviewed repos | 400 | 26.3% |

## Population profile

| Owner type | All repos | Final repos |
| --- | --- | --- |
| Organization | 1,029 (67.6%) | 691 (77.0%) |
| User | 493 (32.4%) | 206 (23.0%) |

| Rank | All repos | Final repos |
| --- | --- | --- |
| 1 | Python: 691 (45.4%) | Python: 524 (58.4%) |
| 2 | TypeScript: 265 (17.4%) | TypeScript: 172 (19.2%) |
| 3 | Unknown: 83 (5.5%) | Jupyter Notebook: 47 (5.2%) |
| 4 | Go: 81 (5.3%) | Go: 35 (3.9%) |
| 5 | Jupyter Notebook: 80 (5.3%) | Rust: 33 (3.7%) |
| 6 | Rust: 66 (4.3%) | JavaScript: 20 (2.2%) |
| 7 | JavaScript: 58 (3.8%) | C++: 17 (1.9%) |
| 8 | C++: 39 (2.6%) | Java: 14 (1.6%) |
| 9 | Java: 29 (1.9%) | C#: 5 (0.6%) |
| 10 | HTML: 18 (1.2%) | HTML: 5 (0.6%) |

Top licenses in the final set:

- `Apache-2.0`: `376`
- `MIT`: `306`
- `NOASSERTION`: `113`
- `AGPL-3.0`: `40`
- `Unknown`: `20`

## Scores and evidence

- Serious score modes: `8` (324), `7` (319), `9` (222), `6` (199), `5` (125)
- AI score modes: `1` (305), `2` (248), `3` (93), `16` (92), `15` (75)
- Repos with manifest paths: `1274` (83.7%)
- Repos with SBOM dependencies: `1151` (75.6%)
- Repos with import-derived dependencies: `57` (3.7%)
- Evidence rows mapped to a known technology: `10435` (2.5%)

## Technology graph inputs

| Metric | Value |
| --- | --- |
| Normalized technology edges | 3,819 |
| Technologies in catalog | 48 |
| Final repos with at least one edge | 826 |
| Final repos with no edge | 71 |

Top technologies in final repos:

| Rank | Technology | Repos | Share of final repos |
| --- | --- | --- | --- |
| 1 | OpenAI SDK | 550 | 61.3% |
| 2 | Transformers | 390 | 43.5% |
| 3 | Anthropic SDK | 275 | 30.7% |
| 4 | LangChain | 260 | 29.0% |
| 5 | Google GenAI SDK | 227 | 25.3% |
| 6 | Accelerate | 214 | 23.9% |
| 7 | LangChain OpenAI Integration | 159 | 17.7% |
| 8 | LiteLLM | 138 | 15.4% |
| 9 | Gradio | 130 | 14.5% |
| 10 | LangGraph | 121 | 13.5% |

Edge category mix:

- `model_access_and_providers`: `1190` (31.2%)
- `orchestration_and_agents`: `852` (22.3%)
- `training_finetuning_and_model_ops`: `809` (21.2%)
- `vector_and_knowledge_storage`: `390` (10.2%)
- `serving_inference_and_local_runtimes`: `250` (6.5%)
- `ui_and_app_frameworks`: `239` (6.3%)
- `observability_tracing_and_monitoring`: `55` (1.4%)
- `evaluation_guardrails_and_safety`: `34` (0.9%)

Final primary segment mix:

- `serving_runtime`: `310` (34.6%)
- `training_finetuning`: `167` (18.6%)
- `orchestration_framework`: `139` (15.5%)
- `vector_retrieval_infrastructure`: `114` (12.7%)
- `agent_application`: `75` (8.4%)
- `rag_search_application`: `51` (5.7%)
- `eval_guardrails_observability`: `14` (1.6%)
- `unassigned`: `10` (1.1%)
- `ai_application`: `6` (0.7%)
- `ai_developer_tool`: `4` (0.4%)
- `developer_tools`: `1` (0.1%)
- `llm_cli_and_chat_application`: `1` (0.1%)
- `inference_optimization`: `1` (0.1%)
- `Image Generation`: `1` (0.1%)
- `developer_tools_plugin`: `1` (0.1%)
- `model_zoo_and_pretrained_models`: `1` (0.1%)
- `AI tool integrations / agent tooling`: `1` (0.1%)

Final repo stars summary:

- min `1,003`, median `5,264`, p75 `13,186`, max `335,512`

Final repo contexts:

- Final repos with manifest paths: `884` (98.6%)
- Final repos with SBOM dependencies: `766` (85.4%)
