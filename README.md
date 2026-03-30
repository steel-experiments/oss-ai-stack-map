# OSS AI Stack Map

`oss-ai-stack-map` is a reproducible research pipeline for mapping the technology stacks used by major open source AI repositories on GitHub.

It discovers candidate repositories, hydrates metadata, collects repo context, extracts direct technology evidence from manifests and GitHub SBOMs, falls back to bounded import scanning when needed, classifies which repos belong in the final AI set, normalizes repo-to-technology edges, and produces summary tables for reporting.

## What this repo currently does

- Discovers candidate repos from GitHub topic queries, description queries, and manual seed repos
- Applies hard scope rules: public, non-fork, non-archived, non-template, recently active, and at least the configured star threshold
- Scores repositories on two axes:
  - serious project vs. tutorial/demo/list material
  - AI relevance based on direct dependency and config evidence
- Extracts direct technology evidence from:
  - manifests for Python, JavaScript/TypeScript, Go, and Rust
  - GitHub dependency graph SBOM exports
  - bounded import scanning for initial ecosystems
- Publishes reviewed low-confidence README fallback edges only when an included repo would otherwise remain unmapped
- Normalizes evidence through a curated alias table of 48 technologies across 8 categories
- Publishes explicit evidence tiers: direct-only, reviewed-fallback, and full-final-population coverage
- Produces Parquet and CSV outputs for downstream analysis
- Supports resumable classification via on-disk checkpoints
- Optionally runs OpenAI judge passes for borderline hardening and stratified sampled final-set validation
- Emits validation audit, robustness, and benchmark split reports alongside the main snapshot outputs

## Current status

This repository implements the first executable slice of the Phase 1 research plan in [SPEC.md](/home/agent/oss-ai-stack-map/SPEC.md) and the methodology described in [docs/methodology.md](/home/agent/oss-ai-stack-map/docs/methodology.md).

Implemented today:

- Discovery
- GraphQL metadata hydration
- Repo context collection from README, tree, manifests, and SBOM
- Rule-based serious-project scoring
- Rule-based AI relevance scoring
- Segment classification
- Technology normalization
- Reporting summary generation
- Snapshot-level descriptive statistics and graph analysis
- Static HTML publication under `docs/`
- Snapshot repair, validation, benchmark recall, and registry suggestion artifacts
- Optional LLM judge modes for borderline hardening and stratified sampled final-set validation
- Evidence-tier, validation-audit, and robustness report artifacts

Still limited:

- Broader ecosystem coverage beyond the current manifest/import support boundary
- A larger benchmark panel beyond the current positive, holdout, and negative-control set

## How the pipeline works

1. `discover`
   Searches GitHub using configured topics, description keywords, and manual seeds, then hydrates repo metadata through the GitHub GraphQL API.
2. `classify`
   Builds a `RepoContext` for each discovered repo by fetching the README, file tree, manifest files, and SBOM, then scores seriousness and AI relevance.
3. `normalize`
   Converts raw dependency evidence into normalized repo-to-technology edges for repos that make the final set.
4. `report`
   Aggregates the staged outputs into headline counts plus top technology and provider prevalence.

The classifier intentionally favors direct evidence over loose topical association. A repo can be clearly adjacent to AI and still be excluded if the current rules do not see enough direct dependency, config, or structural evidence.

## Requirements

- Python `>=3.12`
- [`uv`](https://docs.astral.sh/uv/)
- `GITHUB_TOKEN`
- `OPENAI_API_KEY` only if you enable the optional judge pass

## Quick start

Install dependencies:

```bash
uv sync
```

Create `.env`:

```bash
GITHUB_TOKEN=ghp_...
OPENAI_API_KEY=sk-...  # optional
```

Check GitHub API access:

```bash
uv run oss-ai-stack-map rate-limit
```

Run the full snapshot pipeline:

```bash
uv run oss-ai-stack-map snapshot
```

Summarize a finished run:

```bash
uv run oss-ai-stack-map report --input-dir data/staged
```

## CLI

Inspect rate limits:

```bash
uv run oss-ai-stack-map rate-limit
```

Validate config integrity:

```bash
uv run oss-ai-stack-map validate-config
```

Discovery only:

```bash
uv run oss-ai-stack-map discover
uv run oss-ai-stack-map discover --max-pages-per-query 1 --max-repos 200
```

Classification only:

```bash
uv run oss-ai-stack-map classify
uv run oss-ai-stack-map classify --input-dir data/staged --output-dir data/run-1
uv run oss-ai-stack-map classify --judge
uv run oss-ai-stack-map classify --judge-hardening
uv run oss-ai-stack-map classify --judge-validation
```

End-to-end snapshot:

```bash
uv run oss-ai-stack-map snapshot
uv run oss-ai-stack-map snapshot --max-repos 250 --output-dir data/run-smoke
uv run oss-ai-stack-map snapshot --judge
uv run oss-ai-stack-map snapshot --judge-hardening
uv run oss-ai-stack-map snapshot --judge-validation
```

Summary report:

```bash
uv run oss-ai-stack-map report --input-dir data/staged --top-n 15
```

Compare two repaired snapshots with the current scorecard:

```bash
uv run oss-ai-stack-map snapshot-compare --left-dir data/run-a --right-dir data/run-b
```

Append a scored experiment entry to the ledger:

```bash
uv run oss-ai-stack-map experiment-log \
  --left-dir data/run-a \
  --right-dir data/run-b \
  --lever registry-normalization \
  --files-changed config/technology_registry.yaml \
  --decision keep
```

NetworkX graph analysis for a finished snapshot:

```bash
uv run --with networkx python scripts/networkx_analysis.py
uv run --with networkx python scripts/networkx_analysis.py --input-dir data/run-2026-03-25-resumable --output docs/networkx-analysis-2026-03-25.md
```

## Configuration

All runtime behavior is driven from `config/`:

- [study_config.yaml](/home/agent/oss-ai-stack-map/config/study_config.yaml): snapshot date, thresholds, output behavior, HTTP settings, judge settings, initial ecosystems
- [discovery_topics.yaml](/home/agent/oss-ai-stack-map/config/discovery_topics.yaml): topic queries, description keywords, manual seeds
- [exclusion_rules.yaml](/home/agent/oss-ai-stack-map/config/exclusion_rules.yaml): hard exclusions, educational keywords, excluded directories, supported manifests, source extensions
- [technology_aliases.yaml](/home/agent/oss-ai-stack-map/config/technology_aliases.yaml): canonical technology catalog and alias mapping
- [segment_rules.yaml](/home/agent/oss-ai-stack-map/config/segment_rules.yaml): segment scoring rules and precedence

Default study settings:

- `candidate_stars_min: 1000`
- `major_stars_min: 1000`
- `freshness_months: 1`
- `max_search_pages_per_query: 3`
- hardening and validation judge modes disabled by default
- initial ecosystems: Python, JavaScript, TypeScript, Go, Rust

## Outputs

Runs write staged outputs as Parquet, and CSV when enabled, to the configured output directory.

Core tables:

- `repos.parquet`: discovered repository universe
- `repo_contexts.parquet`: README/tree/manifest/SBOM/import context per repo
- `repo_inclusion_decisions.parquet`: rule and optional judge decisions
- `repo_dependency_evidence.parquet`: raw manifest/SBOM/import evidence
- `repo_technology_edges.parquet`: normalized repo-to-technology edges for included repos
- `technologies.parquet`: canonical technology dimension table
- `judge_decisions.parquet`: optional LLM judge outputs
- `evidence_tier_report.json`: direct-only, reviewed-fallback, and full-population coverage views
- `validation_audit_report.json`: stratified sampled validation audit summary
- `robustness_report.json`: rule-only vs judge-adjusted and evidence-tier robustness checks
- `benchmark_recall_report.json`: positive recall, holdout coverage, and negative-control precision
- `review_queue.json`: machine-readable queue for README-only finals, audit-changed repos, benchmark gaps, and remaining missing-edge finals
- `discovery_stage_timings.parquet`
- `classification_stage_timings.parquet`
- `stage_timings.parquet`

Classification is resumable. Checkpoint batches and run state are written under:

```text
<output-dir>/checkpoints/
```

Raw API responses and judge outputs are cached under snapshot-dated directories in:

```text
data/raw/github_cache/
data/raw/openai_judge/
```

## Analysis artifacts

The repository now includes two data-derived reports for the bundled `2026-03-25` snapshot:

- [descriptive-statistics-2026-03-25.md](/home/agent/oss-ai-stack-map/docs/descriptive-statistics-2026-03-25.md): population, score, evidence, technology, and segment distributions
- [networkx-analysis-2026-03-25.md](/home/agent/oss-ai-stack-map/docs/networkx-analysis-2026-03-25.md): bipartite and projected graph analysis over normalized repo-to-technology edges

The NetworkX report is generated by:

- [networkx_analysis.py](/home/agent/oss-ai-stack-map/scripts/networkx_analysis.py)

That analysis uses `repo_technology_edges.parquet`, so it describes the technology-connected subset of the final included repos rather than every included repo.

## Included snapshot

This repo already contains a completed snapshot at [data/run-2026-03-25-resumable](/home/agent/oss-ai-stack-map/data/run-2026-03-25-resumable).

Based on the included Parquet outputs for `2026-03-25`:

- Discovered repos: `1522`
- Serious repos: `1255`
- AI-relevant repos: `876`
- Final included repos: `876`

Top normalized technology signals in that snapshot:

- OpenAI SDK: `550` repos
- Transformers: `387`
- Anthropic SDK: `275`
- LangChain: `260`
- Google GenAI SDK: `227`

Top provider prevalence:

- OpenAI: `583` repos
- Anthropic: `302`
- Google: `253`

Included reports for that snapshot:

- [run-2026-03-25-summary.md](/home/agent/oss-ai-stack-map/docs/run-2026-03-25-summary.md): short narrative summary
- [descriptive-statistics-2026-03-25.md](/home/agent/oss-ai-stack-map/docs/descriptive-statistics-2026-03-25.md): descriptive statistics from the Parquet outputs
- [networkx-analysis-2026-03-25.md](/home/agent/oss-ai-stack-map/docs/networkx-analysis-2026-03-25.md): graph analysis of normalized technology co-usage

## Methodology notes

The current pipeline uses a conservative definition of "AI repo" and "technology use":

- GitHub is the only live source in this implementation slice
- direct dependency evidence is preferred over ecosystem adjacency
- structural signals are used to reject demos, tutorials, prompt lists, book repos, and notebook-heavy repos
- repo segmentation is heuristic, based on topics, description keywords, config paths, and normalized technologies

Current limitations:

- normalization is curated alias matching, not a generalized package graph
- manifest parsing is strongest for Python, JavaScript/TypeScript, Go, and Rust
- import scanning is intentionally bounded and lower confidence than manifest/SBOM evidence
- some legitimate AI repos can be false negatives if they expose weak dependency evidence
- some borderline educational or showcase repos can still slip through and need calibration

## Development

Run tests:

```bash
uv run pytest -q
```

Lint:

```bash
uv run ruff check
```

The current test suite covers discovery/client behavior, manifest and SBOM parsing, import detection, checkpoint resume behavior, reporting aggregation, and judge decision application.

Compile-check the NetworkX analysis script:

```bash
uv run python -m py_compile scripts/networkx_analysis.py
```

## Repository layout

```text
src/oss_ai_stack_map/
  cli.py                 Typer CLI entrypoints
  config/                runtime config loading
  github/                GitHub API client + caching
  openai/                optional judge integration
  pipeline/              discovery, classification, normalization, reporting
  storage/               Parquet/CSV writes and checkpoint management
config/                  study rules and taxonomy
docs/                    methodology and run notes
tests/                   unit tests
scripts/                 one-off and reusable analysis scripts
data/                    caches, staged outputs, and sample runs
```

## Practical advice

- Use a fresh `--output-dir` for each real run unless you intentionally want checkpoint resume behavior.
- Start with `--max-repos` for smoke tests; full snapshots can take hours and create large caches.
- Keep the snapshot date stable within a run so caches and staged outputs remain coherent.
- Treat the included snapshot as an example dataset, not a frozen truth; the pipeline is designed to be rerun.
