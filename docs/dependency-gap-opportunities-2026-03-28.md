# Dependency-Gap Opportunities By Segment

Date: `2026-03-28`

This note reframes the current snapshot from a different angle than the greenfield project-opportunity memo.

Question:

- if an existing OSS AI project wanted to add obvious next-step dependencies, what categories look systematically under-adopted relative to its segment?

Primary input:

- `data/run-2026-03-25-fresh-1mo-v1`

Method:

- take the validated final set of `783` repos
- group by `primary_segment`
- measure how often each segment already carries the operational tail categories:
  - `observability_tracing_and_monitoring`
  - `browser_and_computer_use_infra`
  - `sandbox_and_isolated_execution`
  - `evaluation_guardrails_and_safety`
  - `runtime_and_agent_deployment`
- infer likely next dependencies from the technologies already adopted by the small minority of peers that do cover those categories

Important interpretation note:

- the specific dependency names below are peer-adoption signals, not a quality ranking
- they are useful as "what adjacent tools do similar projects reach for when they add this capability?"

## Global Pattern

Across the whole final set:

- `534` repos have provider or orchestration coverage
- only `123` repos touch any of the operational-tail categories
- `422` repos have provider or orchestration coverage with none of those operational additions

That is the main dependency-gap signal in the dataset.

## Segment Recommendations

### Serving Runtime

Population:

- `256` repos

Current tail-category coverage:

- observability: `10` (`3.9%`)
- browser infra: `8` (`3.1%`)
- sandboxing: `10` (`3.9%`)
- evaluation: `6` (`2.3%`)
- deployment: `0` (`0.0%`)

Obvious next dependencies:

- observability
- sandboxing
- evaluation
- deployment

Peer-adopted technologies already appearing in this segment:

- observability: `langfuse`, `logfire`
- browser infra: `browserbase`, `browser-use`, `steel-browser`
- sandboxing: `e2b`, `daytona`
- evaluation: `ragas`, `deepeval`, `promptfoo`, `guardrails`

Interpretation:

- serving runtimes already have providers and SDKs
- they mostly have not added the dependencies needed to observe, test, isolate, or deploy agent behavior safely

### Training Finetuning

Population:

- `217` repos

Current tail-category coverage:

- observability: `13` (`6.0%`)
- browser infra: `5` (`2.3%`)
- sandboxing: `8` (`3.7%`)
- evaluation: `6` (`2.8%`)
- deployment: `0` (`0.0%`)

Obvious next dependencies:

- observability
- evaluation
- sandboxing

Peer-adopted technologies already appearing in this segment:

- observability: `logfire`, `langfuse`, `weave`, `phoenix`
- browser infra: `browserbase`, `browser-use`
- sandboxing: `e2b`, `daytona`
- evaluation: `ragas`, `deepeval`, `nemo-guardrails`

Interpretation:

- training-heavy repos have strong model-stack coverage already
- they rarely add experiment observability, workflow eval, or safe execution layers

### Orchestration Framework

Population:

- `99` repos

Current tail-category coverage:

- observability: `16` (`16.2%`)
- browser infra: `10` (`10.1%`)
- sandboxing: `4` (`4.0%`)
- evaluation: `5` (`5.1%`)
- deployment: `0` (`0.0%`)

Obvious next dependencies:

- observability
- browser infra
- evaluation
- sandboxing
- deployment

Peer-adopted technologies already appearing in this segment:

- observability: `langfuse`, `logfire`, `weave`, `phoenix`
- browser infra: `browserbase`, `browser-use`
- sandboxing: `daytona`, `e2b`
- evaluation: `ragas`, `promptfoo`, `guardrails`, `deepeval`

Interpretation:

- orchestration projects are the closest to adding ops layers, but coverage is still low
- the most obvious future dependency set is tracing plus eval plus secure execution

### Vector Retrieval Infrastructure

Population:

- `76` repos

Current tail-category coverage:

- observability: `13` (`17.1%`)
- browser infra: `7` (`9.2%`)
- sandboxing: `6` (`7.9%`)
- evaluation: `5` (`6.6%`)
- deployment: `1` (`1.3%`)

Obvious next dependencies:

- observability
- evaluation
- sandboxing
- deployment

Peer-adopted technologies already appearing in this segment:

- observability: `langfuse`, `logfire`, `weave`, `phoenix`
- browser infra: `browserbase`
- sandboxing: `e2b`, `daytona`
- evaluation: `ragas`, `nemo-guardrails`, `guardrails`, `deepeval`
- deployment: `cloudflare-agents`

Interpretation:

- vector and retrieval projects increasingly sit inside agent pipelines
- their dependency gaps now look more operational than storage-centric

### Agent Application

Population:

- `61` repos

Current tail-category coverage:

- observability: `3` (`4.9%`)
- browser infra: `2` (`3.3%`)
- sandboxing: `1` (`1.6%`)
- evaluation: `0` (`0.0%`)
- deployment: `1` (`1.6%`)

Obvious next dependencies:

- evaluation
- observability
- browser infra
- sandboxing
- deployment

Peer-adopted technologies already appearing in this segment:

- observability: `langfuse`, `phoenix`
- browser infra: `browserbase`
- sandboxing: `e2b`
- deployment: `cloudflare-agents`

Interpretation:

- this is the clearest dependency-gap segment in the whole snapshot
- many agent apps appear to ship without the dependencies needed for tracing, testability, controlled execution, or operational rollout

### AI Developer Tool

Population:

- `34` repos

Current tail-category coverage:

- observability: `0` (`0.0%`)
- browser infra: `1` (`2.9%`)
- sandboxing: `0` (`0.0%`)
- evaluation: `0` (`0.0%`)
- deployment: `0` (`0.0%`)

Obvious next dependencies:

- observability
- evaluation
- sandboxing

Peer-adopted technologies already appearing in this segment:

- browser infra: `browserbase`

Interpretation:

- developer tools are almost entirely focused on SDK and interface layers
- they have barely started pulling in dependencies for reliability, replay, or safe execution

### RAG Search Application

Population:

- `17` repos

Current tail-category coverage:

- observability: `1` (`5.9%`)
- browser infra: `0` (`0.0%`)
- sandboxing: `1` (`5.9%`)
- evaluation: `0` (`0.0%`)
- deployment: `0` (`0.0%`)

Obvious next dependencies:

- evaluation
- observability
- browser infra

Peer-adopted technologies already appearing in this segment:

- observability: `langfuse`
- sandboxing: `daytona`

Interpretation:

- RAG applications have the strongest reason to add eval dependencies, but almost none currently do

## Cross-Cutting Dependency Themes

If we want the most reusable "things existing repos should probably add next" from this data, the cross-cutting set is:

1. tracing and observability
   Current peer signals: `langfuse`, `logfire`, `phoenix`, `weave`
2. agent and workflow evaluation
   Current peer signals: `ragas`, `deepeval`, `promptfoo`, `guardrails`
3. sandboxed execution
   Current peer signals: `e2b`, `daytona`
4. browser capability
   Current peer signals: `browserbase`, `browser-use`, `steel-browser`
5. deployment and runtime control plane
   Current peer signal is still minimal: `cloudflare-agents`

## Bottom Line

The obvious dependency-addition story is not "more provider SDKs."

The obvious dependency-addition story is:

- existing projects already have the model layer
- many also have the orchestration layer
- most still have not added the operational dependencies that make those systems observable, testable, safely executable, and deployable
