# API Serving Strategy

## Goal

Turn `oss-ai-stack-map` from a static report into a public research API that exposes the underlying dataset, evidence graph, snapshot history, and research-quality signals.

The important shift is this:

- not just "report as JSON"
- a queryable research graph with provenance, comparisons, and blind-spot visibility

## Why this project is already a good API candidate

The repo already produces a stable snapshot model, not just an HTML report.

Current snapshot outputs include:

- `repos.parquet`
- `repo_inclusion_decisions.parquet`
- `repo_dependency_evidence.parquet`
- `repo_technology_edges.parquet`
- `technologies.parquet`
- `gap_report.json`
- `benchmark_recall_report.json`
- `technology_discovery_report.json`
- `snapshot_manifest.json`

That means the project already has most of the pieces needed for a real data product:

- entity table: repos
- decision table: inclusion and segment classification
- evidence table: raw dependency evidence
- normalized graph table: repo-to-technology edges
- dimension table: technologies
- research QA artifacts: gaps, benchmarks, discovery candidates, validation

## What makes the API more interesting than the source report

The static report mostly answers:

- what technologies are popular
- what categories are common

A public API can answer better questions:

- Why do you claim this repo uses this technology?
- Which technologies co-occur with MCP, LangGraph, Ollama, PyTorch, or OpenAI SDK?
- How do serving runtimes differ from agent apps or training projects?
- What changed between snapshots?
- Which important benchmark entities are still missing?
- Where is the pipeline still blind or uncertain?

That is the real product upgrade.

## Recommended stack

### Best Cloudflare-native option

Use:

- TypeScript Cloudflare Worker for the API
- Workers Static Assets for `docs/`
- R2 for versioned snapshot bundles and large artifacts
- D1 for the hot queryable relational layer
- KV only for lightweight metadata and caches
- Workers Analytics Engine for request telemetry

### Why this is the best fit

- The project is read-heavy, public, and snapshot-oriented.
- D1 is a good fit for low-latency relational reads at this scale.
- R2 is a better home for Parquet, JSON artifacts, and versioned exports than D1.
- KV should not be the source of truth because it is better suited to cached metadata than structured research queries.
- A TypeScript Worker is a more natural edge runtime for Cloudflare than trying to force the API through Python app compatibility layers.

## Why not default to FastAPI on Cloudflare Workers

Cloudflare does support Python Workers and Python web frameworks, including FastAPI. But for this project, that should be treated as a compatibility option rather than the first choice.

Reasons:

- the public API will mostly be simple read endpoints, not Python-heavy application logic
- Cloudflare-native bindings and storage integrations are best supported from the Worker runtime model
- TypeScript is the path of least resistance for D1, KV, R2, caching, and edge deployment

If the top priority were fastest implementation with maximum reuse of existing Python logic, then a separate `FastAPI + DuckDB` service would be the simpler non-Cloudflare option.

## Data layout recommendation

### Keep in D1

These tables should be loaded into D1 after each published snapshot:

- `snapshots`
- `repos`
- `repo_decisions`
- `technologies`
- `repo_technology_edges`
- `artifact_gap_rows`
- `artifact_benchmark_rows`
- `artifact_discovery_candidates`
- `technology_neighbors`

### Keep in R2

These should remain as versioned snapshot objects in R2:

- full Parquet exports
- full JSON artifacts
- versioned HTML reports
- validation and repair outputs
- large or cold-path evidence blobs
- any future raw context material

### Keep in KV

Use KV only for very small, high-read keys such as:

- `latest_snapshot`
- cached homepage summary payload
- cached facet payloads
- short-lived endpoint response caches

## Publish flow

After each render or snapshot repair:

1. Upload the versioned snapshot bundle to R2.
2. Load API-facing tables into D1.
3. Precompute derived analytical tables.
4. Update `latest_snapshot` in KV.
5. Purge or refresh cached Worker responses.

## Precomputed derived tables worth adding

To keep the public API fast and opinionated, compute these during publish:

- `technology_adoption_by_snapshot`
- `technology_adoption_by_segment`
- `provider_adoption_by_segment`
- `technology_neighbors`
- `gap_candidates`
- `benchmark_status`

These tables are what make the API feel like a research product instead of a thin data dump.

## Endpoint set for v1

The API should be curated, not arbitrary SQL.

### Snapshot endpoints

- `GET /v1/snapshots`
- `GET /v1/snapshots/{snapshot_id}`
- `GET /v1/snapshots/{snapshot_id}/compare/{other_snapshot_id}`

### Repo endpoints

- `GET /v1/repos?q=&segment=&technology_id=&provider_id=&min_stars=&sort=`
- `GET /v1/repos/{repo_id}`
- `GET /v1/repos/{repo_id}/technologies`
- `GET /v1/repos/{repo_id}/evidence`

### Technology endpoints

- `GET /v1/technologies`
- `GET /v1/technologies/{technology_id}`
- `GET /v1/technologies/{technology_id}/repos`
- `GET /v1/technologies/{technology_id}/neighbors`

### Research endpoints

- `GET /v1/providers`
- `GET /v1/segments`
- `GET /v1/gaps`
- `GET /v1/benchmarks`
- `GET /v1/discovery-candidates`
- `GET /v1/exports/{snapshot_id}/{file}`

## Highest-value endpoint

The single most interesting endpoint is probably:

- `GET /v1/technologies/{technology_id}/neighbors`

That endpoint moves the project beyond leaderboard-style reporting and into actual stack composition analysis.

It lets users ask:

- what tends to appear with MCP?
- what is commonly paired with LangGraph?
- what technologies cluster around local-serving repos?

## Suggested response shapes

### Repo detail

Should include:

- repo metadata
- final inclusion flags
- segment info
- normalized technologies
- evidence summaries grouped by source
- links to raw export files where applicable

### Technology detail

Should include:

- technology metadata
- category and provider
- adoption counts
- top repos using it
- segment distribution
- neighboring technologies
- snapshot trend if multiple snapshots exist

### Gap endpoint

Should expose:

- final repos missing edges
- top unmatched packages
- top unmatched prefixes
- suggested discovery inputs
- candidate technologies worth reviewing

This is unusually valuable because it exposes the research system's known blind spots instead of pretending completeness.

## How this becomes materially more interesting

A stronger public version of this project is not just a prettier report. It is a public, evidence-aware API for:

- stack composition
- segment comparison
- provider attribution
- snapshot comparison
- methodology transparency
- research gap inspection

That is more interesting than the original source because users can build with it:

- dashboards
- ecosystem monitors
- startup landscape views
- devrel competitive tracking
- benchmark coverage views
- "what should I use" explorable tools grounded in observed adoption

## Practical implementation recommendation

If building this now, the best path is:

1. Keep the existing Python snapshot pipeline as the source of truth.
2. Add a publish step that writes slim API marts.
3. Push snapshot bundles to R2.
4. Load queryable tables into D1.
5. Serve a typed REST API from a TypeScript Worker.
6. Keep `docs/` on the same Cloudflare deployment as static assets.

## Non-Cloudflare fallback

If Cloudflare-native deployment becomes awkward, the clean fallback is:

- FastAPI
- DuckDB
- Parquet in object storage

That is likely the fastest path to a working API, but not the best long-term fit if the goal is a public edge-served data product on the same domain as the report.
