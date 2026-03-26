# NetworkX Analysis: 2026-03-25 Snapshot

This report analyzes the normalized repo-to-technology graph in `data/run-2026-03-25-resumable` using NetworkX.

Graph construction:

- Bipartite graph: repositories connected to technologies through `repo_technology_edges.parquet`
- Technology projection: two technologies are connected when they co-occur in at least one repo
- Repo projection: two repos are connected when they share at least one normalized technology

Important scope note:

- The final inclusion table contains `876` repos, but only `822` of them have at least one normalized technology edge.
- The NetworkX graph therefore describes the technology-connected subset of the final population, leaving `54` included repos outside the graph.

## Graph overview

| Metric | Value |
| --- | --- |
| Bipartite nodes | 869 |
| Bipartite edges | 3,815 |
| Repo nodes | 822 |
| Technology nodes | 47 |
| Bipartite density | 0.0101 |
| Connected components | 1 |
| Largest component share | 100.0% |
| Technology projection nodes | 47 |
| Technology projection edges | 820 |
| Technology projection density | 0.7586 |
| Repo projection nodes | 822 |
| Repo projection edges | 226,390 |
| Repo projection density | 0.6709 |

The graph is fully connected: every technology-connected repo and every technology sits in a single connected component.

## Degree structure

| Population | Min | P25 | Median | P75 | Mean | Max |
| --- | --- | --- | --- | --- | --- | --- |
| Repo degree | 1 | 2 | 3 | 6 | 4.64 | 25 |
| Technology degree | 1 | 11 | 46 | 107 | 81.17 | 550 |

Top technologies by repo degree:

| Rank | Technology | Category | Repos |
| --- | --- | --- | --- |
| 1 | OpenAI SDK | model_access_and_providers | 550 |
| 2 | Transformers | training_finetuning_and_model_ops | 387 |
| 3 | Anthropic SDK | model_access_and_providers | 275 |
| 4 | LangChain | orchestration_and_agents | 260 |
| 5 | Google GenAI SDK | model_access_and_providers | 227 |
| 6 | Accelerate | training_finetuning_and_model_ops | 214 |
| 7 | LangChain OpenAI Integration | orchestration_and_agents | 159 |
| 8 | LiteLLM | model_access_and_providers | 138 |
| 9 | Gradio | ui_and_app_frameworks | 129 |
| 10 | LangGraph | orchestration_and_agents | 121 |

Top repos by technology count:

| Rank | Repository | Technologies |
| --- | --- | --- |
| 1 | run-llama/llama_index | 25 |
| 2 | langflow-ai/langflow | 23 |
| 3 | Shubhamsaboo/awesome-llm-apps | 23 |
| 4 | ComposioHQ/composio | 21 |
| 5 | traceloop/openllmetry | 19 |
| 6 | coleam00/ottomator-agents | 19 |
| 7 | NVIDIA/GenerativeAIExamples | 19 |
| 8 | MervinPraison/PraisonAI | 18 |
| 9 | Arindam200/awesome-ai-apps | 18 |
| 10 | patchy631/ai-engineering-hub | 17 |

## Technology projection

Weighted degree here means the sum of co-occurrence counts across all neighboring technologies.

Top technologies by weighted degree:

| Rank | Technology | Weighted degree |
| --- | --- | --- |
| 1 | OpenAI SDK | 2,552 |
| 2 | Transformers | 1,962 |
| 3 | LangChain | 1,797 |
| 4 | Anthropic SDK | 1,562 |
| 5 | Google GenAI SDK | 1,436 |
| 6 | LangChain OpenAI Integration | 1,269 |
| 7 | Accelerate | 1,226 |
| 8 | LangGraph | 1,049 |
| 9 | LiteLLM | 1,045 |
| 10 | Chroma | 925 |

Strongest technology co-occurrence pairs:

| Rank | Technology A | Technology B | Shared repos |
| --- | --- | --- | --- |
| 1 | Anthropic SDK | OpenAI SDK | 251 |
| 2 | OpenAI SDK | Transformers | 240 |
| 3 | OpenAI SDK | LangChain | 211 |
| 4 | OpenAI SDK | Google GenAI SDK | 209 |
| 5 | Transformers | Accelerate | 203 |
| 6 | Anthropic SDK | Google GenAI SDK | 154 |
| 7 | LangChain OpenAI Integration | LangChain | 152 |
| 8 | OpenAI SDK | Accelerate | 133 |
| 9 | LangChain OpenAI Integration | OpenAI SDK | 126 |
| 10 | LangChain | Transformers | 124 |
| 11 | OpenAI SDK | LiteLLM | 121 |
| 12 | Transformers | PEFT | 118 |

## Centrality

Betweenness and closeness use inverse co-occurrence weight as distance, so stronger co-usage implies shorter paths.

Because those weighted distances can be smaller than `1`, closeness values here are not bounded to the usual `[0, 1]` range.

Top technologies by betweenness centrality:

| Rank | Technology | Betweenness |
| --- | --- | --- |
| 1 | OpenAI SDK | 0.7991 |
| 2 | Transformers | 0.2849 |
| 3 | LangChain | 0.0957 |
| 4 | Langfuse | 0.0435 |
| 5 | LiteLLM | 0.0011 |
| 6 | Google GenAI SDK | 0.0006 |
| 7 | Anthropic SDK | 0.0006 |
| 8 | LangGraph | 0.0002 |
| 9 | LangChain OpenAI Integration | 0.0002 |
| 10 | LlamaIndex | 0.0002 |

Top technologies by closeness centrality:

| Rank | Technology | Closeness |
| --- | --- | --- |
| 1 | OpenAI SDK | 8.1316 |
| 2 | Transformers | 7.9916 |
| 3 | LangChain | 7.9358 |
| 4 | Anthropic SDK | 7.9010 |
| 5 | Google GenAI SDK | 7.8564 |
| 6 | Accelerate | 7.7779 |
| 7 | LangChain OpenAI Integration | 7.7320 |
| 8 | LiteLLM | 7.6682 |
| 9 | LangGraph | 7.6063 |
| 10 | Ollama | 7.5324 |

Top technologies by eigenvector centrality:

| Rank | Technology | Eigenvector |
| --- | --- | --- |
| 1 | OpenAI SDK | 0.4440 |
| 2 | Transformers | 0.3445 |
| 3 | LangChain | 0.3250 |
| 4 | Anthropic SDK | 0.3073 |
| 5 | Google GenAI SDK | 0.2823 |
| 6 | LangChain OpenAI Integration | 0.2339 |
| 7 | Accelerate | 0.2296 |
| 8 | LangGraph | 0.1956 |
| 9 | LiteLLM | 0.1925 |
| 10 | Chroma | 0.1657 |

OpenAI SDK dominates every centrality family. It is not just the most frequent node; it is also the main bridge technology across otherwise different tool stacks.

## Community structure

Greedy modularity on the technology projection found `2` communities with modularity `0.0970`.

The low modularity value indicates weak separation: the technology graph is dense and heavily cross-linked.

Community 1 (`35` technologies):

- Anthropic SDK, Arize Phoenix, AutoGen, Chainlit, Chroma, CrewAI, DSPy, DeepEval, Google GenAI SDK, Haystack, Helicone, Instructor, LanceDB, LangChain, LangChain Anthropic Integration, LangChain Google GenAI Integration, LangChain OpenAI Integration, LangGraph, Langfuse, LiteLLM, LlamaIndex, Milvus, NeMo Guardrails, Ollama, OpenAI SDK, Promptfoo, PydanticAI, Qdrant, Ragas, Semantic Kernel, Streamlit, Weave, Weaviate, pgvector, smolagents

Community 2 (`12` technologies):

- Accelerate, BentoML, DeepSpeed, Gradio, Guardrails, PEFT, Ray Serve, SGLang, TRL, Transformers, llama.cpp, vLLM

## Category mixing

Most common cross-category co-occurrence blocks by shared-repo weight:

| Rank | Category pair | Weighted co-occurrence |
| --- | --- | --- |
| 1 | `model_access_and_providers` x `orchestration_and_agents` | 1,901 |
| 2 | `orchestration_and_agents` x `orchestration_and_agents` | 1,307 |
| 3 | `model_access_and_providers` x `training_finetuning_and_model_ops` | 986 |
| 4 | `model_access_and_providers` x `vector_and_knowledge_storage` | 906 |
| 5 | `orchestration_and_agents` x `vector_and_knowledge_storage` | 894 |
| 6 | `model_access_and_providers` x `model_access_and_providers` | 872 |
| 7 | `orchestration_and_agents` x `training_finetuning_and_model_ops` | 749 |
| 8 | `training_finetuning_and_model_ops` x `training_finetuning_and_model_ops` | 661 |
| 9 | `model_access_and_providers` x `serving_inference_and_local_runtimes` | 463 |
| 10 | `training_finetuning_and_model_ops` x `vector_and_knowledge_storage` | 455 |

The strongest mixing pattern is between model-provider SDKs and orchestration frameworks, followed by intra-orchestration links and provider/training links.

## Repo projection

The repo projection is extremely dense because many repos share the same dominant technologies.

Top repos by weighted connectivity to other repos:

| Rank | Repository | Weighted degree |
| --- | --- | --- |
| 1 | run-llama/llama_index | 3,138 |
| 2 | langflow-ai/langflow | 3,025 |
| 3 | Shubhamsaboo/awesome-llm-apps | 3,018 |
| 4 | traceloop/openllmetry | 2,953 |
| 5 | coleam00/ottomator-agents | 2,879 |
| 6 | ComposioHQ/composio | 2,851 |
| 7 | microsoft/autogen | 2,735 |
| 8 | patchy631/ai-engineering-hub | 2,731 |
| 9 | mem0ai/mem0 | 2,719 |
| 10 | tensorzero/tensorzero | 2,707 |

## Included repos with no normalized technology edge

`54` final included repos have no row in `repo_technology_edges.parquet`.

These repos are in scope for the classifier, but they are absent from the graph because the current normalization rules found no mapped technology.

Highest-star examples:

| Rank | Repository | Stars | Primary segment | Serious | AI |
| --- | --- | --- | --- | --- | --- |
| 1 | openai/codex | 67,518 | serving_runtime | 9 | 2 |
| 2 | Kong/kong | 43,037 | serving_runtime | 9 | 2 |
| 3 | thedotmack/claude-mem | 40,475 | serving_runtime | 7 | 3 |
| 4 | farion1231/cc-switch | 33,306 | agent_application | 9 | 2 |
| 5 | SillyTavern/SillyTavern | 24,811 | serving_runtime | 8 | 3 |
| 6 | vanna-ai/vanna | 23,107 | vector_retrieval_infrastructure | 7 | 3 |
| 7 | weaviate/weaviate | 15,879 | vector_retrieval_infrastructure | 8 | 3 |
| 8 | RightNow-AI/openfang | 15,518 | serving_runtime | 9 | 3 |
| 9 | memvid/memvid | 13,611 | rag_search_application | 9 | 3 |
| 10 | Lightning-AI/litgpt | 13,263 | serving_runtime | 6 | 3 |
| 11 | rtk-ai/rtk | 13,186 | serving_runtime | 9 | 3 |
| 12 | cft0808/edict | 12,733 | serving_runtime | 8 | 3 |

## Takeaways

1. The technology-connected subset forms a single giant component, which means there is no clear set of isolated AI stack islands in this snapshot.
2. OpenAI SDK is the central hub by frequency, co-occurrence strength, closeness, eigenvector score, and bridge position.
3. The strongest repeated stack pattern is provider SDKs co-occurring with orchestration frameworks, especially around OpenAI, LangChain, Anthropic, Google GenAI, and LangGraph.
4. Community detection finds only weakly separated modules. The graph looks more like one overlapping ecosystem than a set of cleanly partitioned sub-ecosystems.
5. The graph under-represents some included repos because `54` final repos have no normalized technology edge. Any downstream network interpretation should be read as analysis of the normalized-technology subset, not the full final population.
