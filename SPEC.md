# Research Plan: Open Source AI Tech Stack Map (Phase 1)

## 1. Purpose

Build a reproducible, public, GitHub-only research pipeline that answers:

1. **What tech stacks do major open source AI projects use today?**
2. **What do those choices suggest for builders, startups, investors, and developer-relations teams who want to understand the default stack choices in AI?**

This phase is a **current-state snapshot**. It is designed so the same pipeline can be rerun on a quarterly or yearly cadence, which becomes the basis for the later “how is the stack changing over time?” study.

---

## 2. Locked decisions

These are the decisions already made.

- **Primary research question:** what tech stacks do AI projects use?
- **Phase 2 question (deferred):** how is the AI stack changing over time?
- **Scope of projects:** not just infrastructure; include **any serious builder project** on GitHub that meaningfully uses AI.
- **Population:** GitHub only.
- **Inclusion constraints:** public, non-fork, non-archived, freshness filter, stars threshold.
- **License:** recorded as metadata, but **not** used as an inclusion filter.
- **Exclusions:** demos, tutorials, templates, benchmarks-only repos, boilerplates, “awesome lists,” prompt collections, notebook-only educational repos, and other non-production or non-serious repos.
- **Initial ecosystem support:** Python, JavaScript/TypeScript, Go, and Rust in the first production run. Additional ecosystems are added after the first stable snapshot.
- **Evidence sources:** manifest / lockfile / dependency graph / SBOM first. Import evidence is fallback-only. README mentions may be used only for classification, not as primary stack evidence.
- **Published dependency scope:** direct dependencies only.
- **Provider attribution:** provider-specific wrapper packages may count toward provider usage when the mapping is explicit and curated.
- **Outputs for v1:** public dataset, methodology, and static research report. The interactive graph is deferred until the dataset is stable.
- **Validation:** light manual QA on top repos and a random sample.
- **Principle:** keep the pipeline autonomous, reproducible, and not overcomplicated.

---

## 3. High-level study design

The study has three layers:

1. **Universe definition**
   - Define what counts as a major open source AI project.

2. **Stack extraction**
   - Extract direct technology choices from each included repository.

3. **Normalization and publication**
   - Map raw packages/imports into a canonical AI stack taxonomy and publish repo-level plus aggregate outputs.

The design is **discovery-first**. It should not depend on only one worldview of AI (for example, “AI = OpenAI SDK import”). Instead, it should use a broad candidate-discovery strategy and only narrow at the serious-project filtering stage.

---

## 4. Operational definitions

### 4.1 What is an “AI project” for this study?

A repository qualifies as an AI project if its **primary product or primary developer workflow** meaningfully depends on AI/ML/LLM capabilities.

Included examples:

- LLM apps and assistants
- agent frameworks and agent applications
- RAG systems and search/retrieval products
- inference, serving, and model runtime tools
- vector databases and retrieval infrastructure
- evaluation, guardrails, observability, tracing, and prompt tooling
- multimodal apps and tools
- speech, image, video, and code-generation products
- builder tools and developer tools for AI workflows

Excluded examples:

- general data science repos with no meaningful AI product/application focus
- educational or tutorial repos
- one-off demos and examples
- benchmark result repos with no substantial product/tooling code
- template or starter repos
- list repositories and curated-resource repositories

### 4.2 What is a “major” project?

A repository is in scope for the Phase 1 published map if it is:

- public
- non-fork
- non-archived
- serious / production-like
- AI-relevant
- has **>= 1,000 stars**
- has code freshness defined by **`pushed_at` within the last 12 months**

Phase 1 uses the same threshold for discovery and publication. There is no separate 200+ star candidate universe in the first production run.

Reason: the 200+ band increases recall, but it also greatly increases demo/template noise and review cost. For the first public snapshot, precision and reproducibility matter more than long-tail recall.

### 4.3 What is a “tech stack” in this study?

For Phase 1, a tech stack is the set of **direct, observable technology choices** in the repository, measured through:

- package manifests
- lockfiles
- GitHub dependency graph / SBOM export
- import/use statements only when structured dependency data is unavailable or unsupported
- limited infrastructure/config signals where directly declared in repo files

### 4.4 What counts as “use”?

A technology counts as “used” if there is direct evidence from one of the allowed evidence types.

Evidence confidence levels:

1. **High confidence**
   - manifest-declared direct dependency
   - direct dependency from dependency graph / SBOM when directness is explicit

2. **Medium confidence**
   - lockfile evidence tied back to a direct manifest package
   - import evidence in production code

3. **Low confidence**
   - README/description mention only  
   README/description evidence is **not used for published stack edges**. It is only used for project classification and candidate discovery.

### 4.5 Provider-attributed usage

The dataset will publish **two layers**:

1. **Observed technology layer**
   - exact normalized technology/package/tool observed in the repo

2. **Provider-attributed layer**
   - derived provider usage when a curated mapping is explicit

Examples:

- `pip:openai` → observed tech = OpenAI SDK; provider = OpenAI
- `npm:openai` → observed tech = OpenAI SDK; provider = OpenAI
- `pip:langchain-openai` → observed tech = LangChain OpenAI integration; provider = OpenAI
- `npm:@langchain/anthropic` → observed tech = LangChain Anthropic integration; provider = Anthropic
- `langchain` alone → provider = none inferred

Rule: **generic frameworks do not imply a provider** unless the dependency/import explicitly names the provider or maps through a curated provider-specific integration.

---

## 5. Scope and exclusions

### 5.1 In scope

- Public GitHub repositories
- All major ecosystems
- AI infrastructure projects
- AI application projects
- Multi-language monorepos
- Repositories with both open-source and proprietary-service dependencies

### 5.2 Out of scope for Phase 1

- GitLab, Hugging Face-only repos, Bitbucket, self-hosted forges
- Transitive dependency analysis in the published dataset
- Runtime telemetry or real production configuration outside the repository
- Ranking “best” tools
- Prescriptive recommendations beyond popularity and co-occurrence
- Historical trend modeling beyond preserving a repeatable snapshot methodology

### 5.3 Hard exclusions

Exclude repositories if any of the following is true:

- `isFork = true`
- `isArchived = true`
- `isTemplate = true`
- obvious tutorial/demo/example/benchmark/list/template/starter/prompt-pack repo
- repository is overwhelmingly notebooks/docs/example material rather than product/tooling code
- repository does not pass serious-project rules

---

## 6. Data sources

### 6.1 GitHub live metadata
Use GitHub REST and GraphQL APIs for:

- repo identity and canonical URL
- stars
- topics
- description
- language and detected ecosystems
- license
- `isFork`
- `isArchived`
- `isTemplate`
- `createdAt`, `updatedAt`, `pushedAt`
- file tree lookup and targeted file retrieval

### 6.2 GitHub dependency graph / SBOM
Use GitHub’s dependency graph and SBOM export as a scalable source of dependency evidence.

Key notes from GitHub documentation:

- the dependency graph is available for public repos and is on by default
- it is built from manifests and lockfiles
- SBOM can be exported through the GitHub UI or the REST API
- GitHub’s dependency graph supports direct and transitive dependency representations for supported ecosystems

### 6.3 Manifest and lockfile parsing
For direct dependencies, manifest files are the canonical direct-dependency source when available.

Primary files by ecosystem include:

- Python: `pyproject.toml`, `requirements.txt`, `setup.cfg`
- JS/TS: `package.json`, lockfiles
- Go: `go.mod`
- Rust: `Cargo.toml`
- Java/Kotlin: `pom.xml`, `build.gradle`, `build.gradle.kts`
- .NET: `*.csproj`, `packages.config`
- Ruby: `Gemfile`
- PHP: `composer.json`
- Elixir: `mix.exs`
- Swift: `Package.swift`

### 6.4 Code/import detection
Use GitHub code search or targeted file parsing only when structured dependency data is missing or incomplete.

Typical signals:

- imports / require statements
- `from ... import ...`
- framework-specific initialization patterns
- provider-specific namespaces

### 6.5 deps.dev
Use deps.dev for:

- package normalization support
- cross-ecosystem package metadata
- package-to-project enrichment
- purl normalization and related metadata where useful

deps.dev is an enrichment layer, not the sole source of truth for repository stack edges.

### 6.6 GH Archive
Phase 1 is a current-state map, so GH Archive is optional in the primary build. It is mainly used for:

- optional discovery expansion
- later trend analysis
- future quarterly/yearly diffs

If used in Phase 1, GH Archive should help with candidate discovery and audit coverage, not replace live repository metadata as the current-source-of-truth snapshot.

---

## 7. Research output

The study produces two public deliverables in Phase 1.

### 7.1 Public dataset
Core tables:

- `repos`
- `repo_inclusion_decisions`
- `repo_segments`
- `technologies`
- `technology_aliases`
- `repo_technology_edges`
- `qa_reviews`

### 7.2 Research report
A narrative report that answers:

- what the most common AI stack choices are overall
- what the most common choices are by project type
- which technologies co-occur most frequently
- what default choices a builder can infer from the open-source landscape

---

## 8. Recommended stack taxonomy

The taxonomy should be hierarchical and stable enough for repeated snapshots.

### 8.1 Top-level categories

1. **Model access and providers**
   - OpenAI, Anthropic, Gemini/Google, Azure OpenAI, Cohere, Mistral, Groq, Together, Replicate, Hugging Face Inference, local-model APIs

2. **Orchestration and agents**
   - LangChain, LlamaIndex, Haystack, AutoGen, CrewAI, PydanticAI, Semantic Kernel, DSPy, agent runtimes

3. **Retrieval, embeddings, and indexing**
   - embedding SDKs, retrievers, chunking/index libraries, semantic search toolkits

4. **Vector and knowledge storage**
   - Qdrant, Weaviate, Chroma, Milvus, pgvector, Elasticsearch/OpenSearch vector layers, LanceDB

5. **Serving, inference, and local runtimes**
   - vLLM, TGI, Ollama, llama.cpp, SGLang, BentoML, Ray Serve, TorchServe

6. **Training, fine-tuning, and model ops**
   - Transformers, PEFT, TRL, Axolotl, Lightning, DeepSpeed, Accelerate

7. **Evaluation, guardrails, and safety**
   - Ragas, DeepEval, LangSmith client packages, Guardrails, NeMo Guardrails, Promptfoo, eval harnesses with real tooling

8. **Observability, tracing, and monitoring**
   - Langfuse, Arize Phoenix, Helicone, Weave, OpenTelemetry integrations for LLM apps

9. **UI and app frameworks**
   - Gradio, Streamlit, Chainlit, Next.js app integrations, React UI kits for AI products

10. **Deployment and workflow tooling**
   - Docker, Kubernetes indicators, Modal, Vercel, serverless wrappers, workflow orchestrators

11. **Data ingestion and connectors**
   - unstructured/document parsing, connector frameworks, ETL and ingestion layers

### 8.2 Two public views

To avoid one giant and noisy map, publish two views:

#### AI-core stack view
Only categories 1–8 and 11.

#### Full builder-stack view
Categories 1–11, including UI and deployment/workflow signals.

This keeps the study useful for builders while preserving an AI-focused core.

---

## 9. Canonical package and technology normalization

### 9.1 Why normalization is required

The same technology appears under different package names in different ecosystems.

Examples:

- Python SDK vs npm SDK
- official provider SDK vs framework integration package
- vendor package vs namespace-specific wrapper
- package names that map to one canonical technology concept

### 9.2 Canonical technology table

Create a `technologies` table with:

- `technology_id`
- `display_name`
- `vendor`
- `category_id`
- `subcategory`
- `is_provider`
- `is_open_source`
- `homepage_url`
- `notes`

### 9.3 Alias table

Create a `technology_aliases` table with:

- `alias_id`
- `technology_id`
- `alias_type`
  - `purl_exact`
  - `package_exact`
  - `package_regex`
  - `import_exact`
  - `import_regex`
  - `namespace_prefix`
  - `action_slug`
  - `config_signal`
- `alias_value`
- `provider_id` (nullable)
- `confidence_default`
- `notes`

### 9.4 Normalization rules

1. Exact purl match beats everything else.
2. Exact package name match beats regex match.
3. Namespace/prefix rules are allowed only when precision is known to be high.
4. Provider attribution is published only when explicit.
5. Generic frameworks do not imply providers.
6. Unknown packages remain in raw evidence and are marked unmapped for later review.

### 9.5 Suggested curation strategy

- hand-curate the top 300–500 AI-relevant technologies first
- leave long tail packages as raw/unmapped initially
- after the first run, review the most frequent unmapped packages and expand the alias table
- treat alias-table expansion as a versioned artifact

---

## 10. Population building methodology

### 10.1 Candidate discovery principles

Candidate discovery must avoid narrow bias. Use the union of:

1. **metadata/topic discovery**
2. **keyword discovery**
3. **manual seed list of iconic projects**
4. **one controlled expansion pass from observed misses**

### 10.2 Metadata/topic discovery

Maintain a config of AI topics and repo-description keywords such as:

- `llm`
- `agents`
- `ai-agent`
- `rag`
- `retrieval-augmented-generation`
- `vector-database`
- `multimodal`
- `inference`
- `serving`
- `evaluation`
- `guardrails`
- `speech-to-text`
- `text-to-speech`
- `image-generation`
- `video-generation`
- `copilot`
- `assistant`
- `model-context-protocol`
- `mcp`

This is recall-first. The serious-project filter handles precision later.

### 10.3 Manual seed list

Add a hand-curated seed list of canonical high-visibility repos to reduce cold-start misses.

Examples of repo types to seed manually:

- iconic frameworks
- vector stores
- serving runtimes
- AI app frameworks
- widely used assistants / copilots / agent apps

### 10.4 Controlled discovery expansion

After the first QA pass:

1. review missed high-star repos and false negatives
2. add missing keywords/topics or manual seeds
3. adjust exclusion rules only when the QA error is systematic
4. rerun discovery once

Do not add package-family search expansion in Phase 1. Limit this to **one controlled expansion round** to avoid uncontrolled scope creep.

---

## 11. Serious-project filtering methodology

This is the most important part of Phase 1.

### 11.1 Hard metadata filters

A repo must satisfy:

- public repo
- not archived
- not fork
- not template
- stars >= 1,000
- `pushed_at` within freshness window

### 11.2 Hard exclusion patterns

Exclude if repo name, description, topics, or README strongly indicate:

- demo
- example
- tutorial
- awesome
- list
- template
- starter
- boilerplate
- workshop
- course
- benchmark-only
- prompt collection
- paper list
- cheatsheet
- playground

Do **not** exclude purely on one keyword if the structural signals show that the repository is a substantial tool/product. Example: an evaluation framework that contains the word “benchmark” may still be a serious product repository.

### 11.3 Structural positive signals

Use the following deterministic scoring rule:

- `+2` dependency manifest present outside excluded directories
- `+1` lockfile present outside excluded directories
- `+2` non-trivial code tree under `src/`, `app/`, `packages/`, `pkg/`, `server/`, `cmd/`, or equivalent
- `+1` tests present
- `+1` CI/workflow config present
- `+1` release tags or release workflow present
- `+1` multiple package/application subdirectories in a monorepo
- `+1` docs oriented toward installation/usage rather than course/tutorial material

### 11.4 Structural negative signals

- `-3` repository is almost entirely notebooks, docs, slides, or images
- `-3` source tree exists only under `examples/`, `demo/`, or `tutorials/`
- `-2` no manifests, no tests, no CI, and trivial code footprint
- `-2` only prompt files or notebooks with minimal supporting code

### 11.5 Serious-project pass rule

- start at `0`
- add positive points
- subtract negative points
- fail immediately on hard exclusion
- require at least one positive code-footprint signal
- pass serious-project filter if score `>= 3`

### 11.6 Directory-level exclusions

Ignore evidence under directories such as:

- `examples/`
- `example/`
- `demo/`
- `demos/`
- `tutorial/`
- `tutorials/`
- `benchmarks/`
- `benchmark/`
- `docs/`
- `notebooks/`
- `vendor/`
- `third_party/`
- `tests/` (for import evidence only)
- `.venv/`
- `node_modules/`

Monorepo rule: prefer manifests and source under root app directories, not examples.

---

## 12. AI relevance classification

A repo must pass the serious-project filter and also be AI-relevant.

### 12.1 AI relevance signals

Use the following deterministic scoring rule:

- `+3` direct dependency on a known AI technology from a manifest
- `+3` provider-specific integration dependency from a manifest
- `+2` direct dependency from dependency graph / SBOM when directness is explicit
- `+2` AI-serving, inference, or vector-runtime config file directly declared in repo
- `+1` AI topic in repo metadata
- `+1` AI-oriented description keyword
- `+1` import evidence of AI SDK/framework in non-example production code

Metadata-only evidence is never enough by itself unless both topic and description strongly indicate a known AI product category and the repo also passes the serious-project filter with a score of `>= 5`. Any such fallback case must be marked for QA review.

### 12.2 Recommended AI relevance rule

- **Strong pass:** any repo with `>= 3` points and at least one dependency/config/import signal
- **Fallback pass:** any repo with `>= 4` points, including both metadata signals and at least one non-metadata signal
- **Fail:** metadata-only repos

This keeps the study open-minded but prevents generic software repos from entering the final set on keyword noise alone.

---

## 13. Dependency extraction methodology

### 13.1 Canonical published edge type

The published dataset will contain **direct repository → technology edges** only.

Each edge must include:

- repo ID
- technology ID
- category
- raw package/import/config signal
- evidence type
- evidence source file/path
- directness flag = true
- confidence
- provider attribution if applicable
- snapshot date

### 13.2 Extraction order

For each included repo, run the following extraction pipeline:

1. retrieve GitHub dependency graph / SBOM if available
2. list files on the default branch
3. fetch and parse manifests and lockfiles
4. build canonical direct dependency candidates from manifests
5. use SBOM/dependency graph to enrich version/purl/license info and to audit completeness
6. run targeted import detection if structured dependency data is missing or incomplete
7. emit normalized repo → technology edges

### 13.3 Important design choice for direct dependencies

Because the final published dataset is **direct dependencies only**, manifest-declared direct dependencies are the preferred canonical source of directness whenever available.

Use SBOM/dependency graph as:

- a scalable first-pass source
- a consistency check
- an enrichment source
- a fallback when manifests are unavailable or unsupported

### 13.4 Dependency scopes

Track dependency scope with values such as:

- `runtime`
- `prod`
- `optional`
- `dev`
- `test`
- `build`
- `unknown`

Primary public rankings should use:

- `runtime`
- `prod`
- `optional` if clearly product-relevant

Keep `dev/test/build` in raw data but exclude them from the main public “most popular stack choices” rankings unless a separate developer-tooling appendix is desired.

### 13.5 Monorepo handling

For monorepos:

- parse every relevant manifest outside excluded directories
- tag each manifest with `subproject_path`
- deduplicate identical technologies within the same repo for repo-level prevalence
- keep subproject-level detail in the raw tables

### 13.6 Unsupported ecosystems

If the repo uses an unsupported or partially supported ecosystem:

1. attempt manifest parsing via custom parser
2. if unavailable, use import detection
3. mark the edge with lower confidence
4. record the missing parser gap for future extension

---

## 14. Import detection methodology

Import detection is a fallback and audit mechanism, not the first source of truth and not part of the normal happy path for supported ecosystems.

### 14.1 When to use it

Use import detection if:

- no manifest/lockfile is found in an otherwise serious repo
- the ecosystem is unsupported or partially supported
- SBOM is unavailable or clearly incomplete
- a provider or framework integration is known to be commonly imported but not represented in manifest extraction

### 14.2 Search scope

Restrict import detection to production-code paths and ignore excluded directories.

### 14.3 Import evidence examples

Examples of patterns:

- Python:
  - `import openai`
  - `from openai import`
  - `import anthropic`
  - framework integration imports
- JS/TS:
  - `from "openai"`
  - `require("openai")`
  - scoped integration packages
- Go/Rust/Java:
  - provider-specific package namespaces

### 14.4 Confidence rules

- provider/framework import in non-example production code = medium confidence
- import only in tests/examples = ignore
- import only in docs snippets = ignore

---

## 15. Provider attribution rules

Provider attribution is valuable but should stay conservative.

### 15.1 Count a provider when

- the official provider SDK is directly depended on
- a provider-specific integration package is directly depended on
- a provider-specific import appears in production code and no higher-confidence evidence exists

### 15.2 Do not count a provider when

- only a generic framework core package is present
- only a generic “OpenAI-compatible” abstraction is present
- the provider inference depends on runtime config not visible in repo
- the evidence is only a README mention

### 15.3 Publish two metrics

For each provider publish:

1. **Observed provider package prevalence**
2. **Derived provider attribution prevalence**

This prevents confusion between exact package counts and provider-level rollups.

---

## 16. Suggested repository segments

Segment the included repo universe into a small number of practical buckets.

Recommended segments:

- AI application
- agent application
- RAG/search application
- AI developer tool
- orchestration / framework
- serving / runtime
- vector / retrieval infrastructure
- evaluation / guardrails / observability
- multimodal / speech / media
- training / fine-tuning
- general builder platform

Allow multi-label classification, but force one `primary_segment` for simple charts.

Primary segment assignment must be deterministic:

- assign one point for each matched segment rule from metadata, dependencies, and config signals
- choose the highest-scoring segment as `primary_segment`
- break ties using this precedence order:
  1. serving / runtime
  2. vector / retrieval infrastructure
  3. orchestration / framework
  4. evaluation / guardrails / observability
  5. training / fine-tuning
  6. multimodal / speech / media
  7. RAG/search application
  8. agent application
  9. AI developer tool
  10. AI application
  11. general builder platform

---

## 17. Metrics and ranking logic

### 17.1 Primary metrics

For each technology:

- `repo_count`: number of included repos with the technology
- `repo_share`: `repo_count / total_included_repos`
- `weighted_repo_count`: sum of `sqrt(stars)` across repos using the technology
- `segment_repo_share`: share within each segment
- `language_repo_share`: share within each primary language/ecosystem band

### 17.2 Why use both unweighted and weighted metrics

Use two views:

- **unweighted prevalence** shows how common a technology is across projects
- **star-weighted prevalence** emphasizes technologies used by more visible/popular projects without letting a few huge repos dominate as much as raw star-summing would

Use `sqrt(stars)` weighting by default for secondary charts.

### 17.3 Co-occurrence metrics

For tech-pair analysis compute:

- pair count
- pair share
- pointwise lift / observed-over-expected ratio
- co-occurrence network community detection

This helps reveal recurring stack bundles such as:

- provider + orchestration
- RAG + vector store
- runtime + serving layer
- app framework + observability layer

## 18. Data model specification

### 18.1 `repos`

| field | type | notes |
|---|---|---|
| repo_id | string | canonical GitHub repo ID |
| full_name | string | `owner/name` |
| html_url | string | canonical repo URL |
| description | string | repo description |
| owner_type | string | user/org |
| stars | int | stargazers count |
| forks | int | fork count |
| primary_language | string | GitHub primary language |
| topics | array<string> | repo topics |
| license_spdx | string | recorded, not a filter |
| is_archived | bool | hard filter |
| is_fork | bool | hard filter |
| is_template | bool | hard filter |
| created_at | timestamp | metadata |
| updated_at | timestamp | metadata |
| pushed_at | timestamp | freshness source |
| snapshot_date | date | study snapshot |

### 18.2 `repo_inclusion_decisions`

| field | type | notes |
|---|---|---|
| repo_id | string | FK |
| passed_candidate_filter | bool | broad set |
| passed_serious_filter | bool | serious-project gate |
| passed_ai_relevance_filter | bool | AI gate |
| passed_major_filter | bool | final major set |
| score_serious | int | serious score |
| score_ai | int | AI relevance score |
| exclusion_reason | string | nullable |
| notes | string | reviewer or rule notes |

### 18.3 `technologies`

| field | type | notes |
|---|---|---|
| technology_id | string | canonical ID |
| display_name | string | human-readable |
| vendor | string | provider/vendor |
| category_id | string | taxonomy category |
| subcategory | string | optional |
| is_provider | bool | provider node |
| is_open_source | bool | optional metadata |
| homepage_url | string | optional |

### 18.4 `repo_technology_edges`

| field | type | notes |
|---|---|---|
| repo_id | string | FK |
| technology_id | string | FK |
| raw_signal | string | raw package/import/config value |
| raw_version | string | nullable |
| evidence_type | string | `manifest`, `sbom`, `lockfile`, `import`, `config` |
| evidence_path | string | file path or query |
| direct | bool | always true in published dataset |
| dependency_scope | string | `runtime`, `dev`, etc. |
| confidence | string | `high`, `medium`, `low` |
| provider_id | string | nullable |
| subproject_path | string | for monorepos |
| snapshot_date | date | snapshot version |

### 18.5 `qa_reviews`

| field | type | notes |
|---|---|---|
| review_id | string | PK |
| repo_id | string | FK |
| reviewer | string | initials or system |
| sample_type | string | `top_repo`, `random`, `exception` |
| inclusion_correct | bool | QA label |
| edge_precision_estimate | float | optional |
| notes | string | free text |

---

## 19. Recommended repository layout for implementation

```text
oss-ai-stack-map/
  README.md
  pyproject.toml
  uv.lock
  config/
    study_config.yaml
    discovery_topics.yaml
    technology_aliases.yaml
    exclusion_rules.yaml
    segment_rules.yaml
  data/
    raw/
    staged/
    published/
  src/
    discover/
      github_search.py
      metadata_fetch.py
      candidate_union.py
    classify/
      serious_filter.py
      ai_relevance.py
      segment_classifier.py
    extract/
      sbom_fetch.py
      tree_fetch.py
      manifest_fetch.py
      import_search.py
      parsers/
        python.py
        javascript.py
        go.py
        rust.py
    normalize/
      purl_normalize.py
      alias_match.py
      provider_attribution.py
    aggregate/
      metrics.py
      cooccurrence.py
    publish/
      export_tables.py
      report_tables.py
    qa/
      sample_repos.py
      review_merge.py
  docs/
    methodology.md
    report.md
```

---

## 20. Recommended runtime architecture

Keep the system simple.

### 20.1 Core choices

- **Python + Astral `uv`** for ETL, parsing, and reproducible environment management
- **DuckDB + Parquet** for local analytics and reproducible outputs
- **GitHub APIs** for metadata/SBOM/file retrieval
- **Optional BigQuery** only when GH Archive is added for trend work
- **GitHub Actions or CI scheduler** for repeatable reruns

### 20.2 Why DuckDB + Parquet

This keeps the project:

- lightweight
- easy to reproduce locally
- cheap to publish
- easy to diff between snapshots
- easy to feed into a frontend explorer

### 20.3 Caching and retries

The implementation should cache:

- repo metadata responses
- SBOM responses
- file-tree listings
- fetched manifests
- import-search results

Use deterministic snapshot folders keyed by `snapshot_date`.

### 20.4 Initial support boundary

The first production run should fully support:

- Python
- JavaScript / TypeScript
- Go
- Rust

Other ecosystems may still be included if evidence is available from SBOM or simple manifest parsing, but they are not required to block the first publishable snapshot.

---

## 21. Example config

```yaml
snapshot_date: 2026-03-25

filters:
  candidate_stars_min: 1000
  major_stars_min: 1000
  freshness_months: 12
  include_public_only: true
  exclude_forks: true
  exclude_archived: true
  exclude_templates: true

classification:
  serious_pass_score: 3
  ai_relevance_pass_score: 3
  publish_direct_dependencies_only: true
  readme_mentions_used_for_edges: false

outputs:
  publish_major_view: true
  publish_emerging_view: false
  include_dev_dependencies_in_primary_rankings: false
  publish_provider_rollups: true
  publish_interactive_graph: false

ecosystems:
  initial:
    - python
    - javascript
    - typescript
    - go
    - rust
```

---

## 22. Pipeline stages

### Stage A — discover candidates
Output: candidate table.

Tasks:

1. run metadata/topic discovery queries
2. run keyword discovery queries
3. add manual seed repos
4. union all repos
5. fetch canonical metadata
6. deduplicate by repo ID

### Stage B — apply hard filters
Output: filtered candidate table.

Tasks:

1. drop forks
2. drop archived repos
3. drop templates
4. drop repos below candidate star threshold
5. drop repos outside freshness window

### Stage C — serious-project scoring
Output: serious-project decision table.

Tasks:

1. fetch README and file tree
2. compute positive and negative structural signals
3. apply hard exclusion patterns
4. compute serious score
5. emit pass/fail + reason

### Stage D — AI relevance scoring
Output: AI-relevance decision table.

Tasks:

1. inspect metadata topics and description
2. inspect structured dependency signals
3. inspect import evidence when needed
4. compute AI relevance score
5. emit pass/fail + reason

### Stage E — major-set construction
Output: final major repo universe.

Tasks:

1. require serious pass
2. require AI relevance pass
3. require `stars >= major_stars_min`

### Stage F — extract stack evidence
Output: raw repo evidence tables.

Tasks:

1. fetch SBOM when possible
2. fetch manifests and lockfiles
3. parse direct dependencies
4. enrich with versions and purls
5. fallback to import detection
6. keep evidence provenance

### Stage G — normalize and map
Output: canonical repo → technology edges.

Tasks:

1. purl/package normalization
2. alias matching
3. provider attribution
4. category tagging
5. confidence assignment

### Stage H — aggregate and analyze
Output: aggregate tables.

Tasks:

1. prevalence rankings
2. segment-level rankings
3. co-occurrence graph
4. unknown/unmapped review queues

### Stage I — QA
Output: reviewed sample and issue list.

Tasks:

1. review top repos by stars
2. review random sample
3. log false positives / false negatives
4. refine exclusion rules and aliases
5. rerun once if needed

### Stage J — publish
Output: final public dataset and report.

Tasks:

1. write versioned Parquet/CSV exports
2. generate report tables and charts
3. publish methodology and caveats
4. tag snapshot version

---

## 23. Execution budget and rate-limit model

### 23.1 Engineering effort estimate

Assuming one engineer and a scoped first production run:

- MVP pipeline with real outputs: roughly **3-5 working days**
- stable, reusable pipeline with caching, QA workflow, and publishable outputs: roughly **1-2 weeks**
- broader ecosystem support and interactive frontend: defer until after the first stable snapshot

These are planning estimates, not commitments.

### 23.2 Working-set estimate

Plan for a working universe on the order of **hundreds of repositories**, not thousands, once the `stars >= 1,000` and freshness filters are applied.

The exact count is intentionally not assumed in the methodology. The implementation should log:

- number of repos returned by discovery
- number surviving hard filters
- number passing serious-project scoring
- number passing AI relevance scoring

### 23.3 GitHub API budget assumptions

Design the pipeline to stay well under normal authenticated GitHub API limits by:

- using GraphQL for batched metadata hydration
- using REST selectively for trees, contents, and SBOM retrieval
- caching every response by `snapshot_date`
- avoiding code search in the normal extraction path
- limiting concurrent requests to a small worker pool

At planning time, assume:

- authenticated REST and GraphQL requests are sufficient for a full run if caching is enabled
- search and code-search endpoints are the most rate-limit-sensitive parts of the system
- secondary limits matter more than hourly caps if concurrency is too aggressive

### 23.4 Concurrency and retry policy

Use a conservative default client policy:

- `5-10` concurrent requests maximum
- exponential backoff with jitter on `403`, `429`, and abuse-detection responses
- inspect `/rate_limit` periodically and slow down when search-related buckets become tight
- never require code search for the core published dataset

### 23.5 Runtime expectation

With caching, a rerun of the same snapshot should be materially faster than the first full snapshot build.

The first full snapshot should be expected to complete in:

- minutes for discovery and metadata hydration
- longer for manifest/tree/SBOM extraction depending on repo count and cache warmth

The implementation must emit per-stage timing so actual runtime can replace these planning assumptions after the first run.

---

## 24. Quality assurance specification

### 24.1 QA sample design

Run light manual QA on:

- **top 25** repos by stars in the final major set
- **random 25** repos from the remainder
- all exception cases with contradictory signals

### 24.2 QA questions

For each reviewed repo, answer:

1. should this repo be included in the final major set?
2. is the primary segment correct?
3. are the top extracted technologies materially correct?
4. are any major technologies missing?
5. are any detected technologies false positives?
6. is provider attribution correct?

### 24.3 Recommended targets

Targets for Phase 1:

- inclusion precision > 90%
- repo-to-technology edge precision > 85% on reviewed edges
- provider attribution precision > 90% on reviewed attributions
- unknown/unmapped share reduced substantially after first alias review

These are operational goals, not publication claims unless actually measured.

---

## 25. Reporting specification

The public report should include:

1. **Methodology summary**
2. **Definition of “major AI repo”**
3. **Coverage summary**
4. **Top technologies overall**
5. **Top technologies by segment**
6. **Top providers and provider-attributed rollups**
7. **Top vector, orchestration, serving, eval, and UI choices**
8. **Common stack bundles**
9. **Representative repo case studies**
10. **How builders should read the results**
11. **Limitations and caveats**
12. **Future trend-study plan**

### 25.1 Recommended charts

- top-20 technologies bar charts
- category-by-technology heatmaps
- provider rollup tables
- technology co-occurrence network
- segment-level comparison charts

Interactive exploration is deferred until after the first stable dataset and report are validated.

---

## 26. Interpretation guidance for the final report

The report should explicitly say:

- popularity is not the same as quality
- public OSS behavior is not identical to private startup behavior
- repository dependencies do not perfectly reveal runtime production usage
- the study is most useful as a picture of **default builder choices and common stack bundles**
- the results are better for identifying **what is common** than proving **what is best**

This keeps the research useful without overclaiming.

---

## 27. Key risks and mitigations

### Risk 1: candidate-discovery bias
If discovery relies too much on one topic vocabulary or manual seed list, the sample will be biased.

**Mitigation:** use multiple metadata discovery channels plus one controlled expansion pass.

### Risk 2: false positives from demos/tutorials
AI repos are crowded with examples and templates.

**Mitigation:** strong serious-project filter, directory exclusions, QA on top repos.

### Risk 3: direct-vs-runtime mismatch
A declared dependency may not reflect actual production use.

**Mitigation:** publish the exact operational definition and avoid stronger claims than the evidence supports.

### Risk 4: provider over-attribution
Wrappers may make provider inference noisy.

**Mitigation:** only publish provider attribution when explicitly curated; publish observed-tech and provider layers separately.

### Risk 5: monorepo noise
Examples or subpackages may pollute repo-level conclusions.

**Mitigation:** path-level exclusions and subproject-aware parsing.

### Risk 6: ecosystem coverage gaps
Some ecosystems will have weaker parser support.

**Mitigation:** track evidence type and confidence per edge; prioritize common ecosystems first but do not exclude others if evidence exists.

---

## 28. Phase 2 extension path

This plan intentionally sets up future trend work.

Future longitudinal study additions:

- snapshot diffs across quarters/years
- GH Archive trend features
- “new entrant” technology detection
- rising/falling stack bundles
- migration paths (for example, one provider stack to another)
- segment-specific trend views

The Phase 1 output should therefore be **versioned by snapshot date** and fully reproducible.

---

## 29. Final implementation recommendation

Use the following simple, defensible rule set for the first production run:

1. **Build a candidate universe** from metadata/topic discovery, keyword discovery, and a manual seed list.
2. **Use live GitHub metadata** for current inclusion decisions.
3. **Define “major” as 1,000+ stars and pushed within 12 months** for both discovery and publication in Phase 1.
4. **Use explicit serious-project and AI-relevance scoring rules** rather than reviewer intuition.
5. **Use manifest-declared direct dependencies as the canonical direct-dependency source**, with SBOM/dependency graph as enrichment and fallback.
6. **Publish exact technology usage and provider-attributed usage separately.**
7. **Limit first-class parser support to Python, JavaScript/TypeScript, Go, and Rust** in the first production run.
8. **Use Python with `uv`, DuckDB, and Parquet** so the pipeline is easy to reproduce and rerun.
9. **Publish a versioned public dataset plus methodology and static report** so the research can be rerun on a schedule.

That is the simplest design that still produces a credible and useful AI tech-stack map.

---

## 30. Expected outcomes

If executed correctly, this study will produce:

- a defensible public list of major open source AI projects on GitHub
- a clean canonical map of the most common AI stack choices across those projects
- provider, framework, vector-store, serving, eval, observability, UI, and builder-stack prevalence tables
- common co-occurring stack bundles
- reusable infrastructure for later trend research
- a methodology that is simple enough to rerun and explain publicly

---

## 31. References

These sources support the implementation choices around GitHub dependency data, SBOM export, GitHub code search, GH Archive, and deps.dev.

1. GitHub Docs — About the dependency graph  
   https://docs.github.com/en/code-security/concepts/supply-chain-security/about-the-dependency-graph

2. GitHub Docs — How the dependency graph recognizes dependencies  
   https://docs.github.com/en/code-security/concepts/supply-chain-security/dependency-graph-data

3. GitHub Docs — Dependency graph supported package ecosystems  
   https://docs.github.com/en/code-security/reference/supply-chain-security/dependency-graph-supported-package-ecosystems

4. GitHub Docs — Exporting a software bill of materials for your repository  
   https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/exporting-a-software-bill-of-materials-for-your-repository

5. GitHub Docs — REST API endpoints for software bill of materials (SBOM)  
   https://docs.github.com/en/rest/dependency-graph/sboms

6. GitHub Docs — GitHub code search syntax  
   https://github.com/github/docs/blob/main/content/search-github/github-code-search/understanding-github-code-search-syntax.md

7. GitHub Docs — GraphQL repository fields and ordering enums  
   https://docs.github.com/en/graphql/reference/interfaces  
   https://docs.github.com/en/graphql/reference/enums

8. GitHub Docs — REST API endpoints for starring  
   https://docs.github.com/en/rest/activity/starring

9. GH Archive  
   https://www.gharchive.org/

10. GitHub Docs — Rate limits for the REST API  
   https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api

11. GitHub Docs — Rate limits and query limits for the GraphQL API  
   https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api

12. deps.dev documentation  
   https://docs.deps.dev/  
   https://docs.deps.dev/bigquery/v1/
