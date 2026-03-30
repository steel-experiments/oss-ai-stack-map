# OSS AI Stack Map: Publication Findings And Interpretation

Snapshot: `data/run-2026-03-29-methodology-v4`

Publication report: `docs/oss-ai-stack-report-2026-03-29-v2.html`

## Scope

This publication describes a GitHub-only snapshot of major open source AI repositories that are public, non-fork, non-archived, active within 1 month, and above the project’s star threshold.

The current validated publication snapshot contains:

- `1,522` discovered repos
- `981` final major AI repos
- `5,186` normalized repo-technology edges
- `66` tracked technologies
- `49` positive benchmark entities, `12` holdout entities, and `10` negative controls with `0` failed thresholds

Judge usage is explicit rather than hidden:

- `651` repos were judge-reviewed in either hardening or validation mode
- `157` judge overrides were applied
- the publication includes a seeded 10% validation sample of `99` final repos
- the validation report is clean: `status = ok`

## Headline Findings

1. The major OSS AI stack is provider-first.
The `OpenAI` provider family appears in `546` final repos, `Anthropic` in `290`, and `Google GenAI` in `215`. Provider-category edges account for `23.2%` of all normalized repo-technology edges.

2. Multi-provider adoption is common, not exceptional.
`301` of `975` technology-mapped final repos (`30.9%`) use at least two tracked providers. The most common provider pairing is `OpenAI` plus `Anthropic` in `244` repos, followed by `OpenAI` plus `Google GenAI` in `215` repos.

3. The ecosystem is backend-heavy.
The largest primary segments are `serving_runtime` (`296` repos), `training_finetuning` (`283`), and `orchestration_framework` (`122`). On normalized edge volume, training/model-ops, providers, and orchestration dominate the stack.

4. Retrieval is established infrastructure, not a niche layer.
`vector_retrieval_infrastructure` is the primary segment for `99` final repos, and the snapshot shows a real retrieval layer rather than a marginal one, with substantial adoption across vector databases and related retrieval tooling.

5. Evaluation and observability remain thin relative to the rest of the stack.
The snapshot contains only `32` evaluation/guardrail edges and `70` observability edges. Compared with provider, training, orchestration, and retrieval layers, these categories are still visibly underrepresented in what major OSS projects expose directly in their repos.

6. The publication artifact is now stronger on evidence quality than earlier runs.
Only `3` final repos remain without a normalized technology edge, all of them because they have dependency evidence that still does not normalize cleanly. `41` final repos still rely on reviewed README-only fallback edges, so fallback coverage remains bounded and inspectable rather than hidden.

7. The benchmark layer now measures more than recall.
The benchmark panel now separates a main positive panel, a holdout slice, and exact negative controls. In this snapshot, negative-control exclusion and holdout coverage both pass cleanly, which makes the benchmark a more defensible quality-control layer than a pure anchor checklist.

## Interpretation

The modern major OSS AI repo is not best described as a thin wrapper around a single model vendor. The dominant pattern is a compositional stack: a provider SDK or access layer, training or inference runtime pieces, orchestration, and often some retrieval substrate. That is a more plural and infrastructure-heavy picture than “one-framework ecosystems” suggest.

The segment mix also suggests that the center of gravity is still systems and platform work, not end-user applications. `747` of the `980` final repos are organization-owned, and the language mix is led by `Python` (`540` repos) and `TypeScript` (`206`), with `Go` and `Rust` also materially present. This looks like an ecosystem of builders shipping platforms, runtimes, orchestration layers, model tooling, and developer-facing surfaces.

Methodologically, the project is now strong enough for public interpretation as a research map. The benchmark panel is broader, the report is aligned to the actual 1-month study frame, evidence tiers are explicit, the validation sample is explicit and audited, and the stable publication target points at the methodology-upgraded validated snapshot rather than an older repaired run. The right public claim is still “research-grade ecosystem map,” not “complete census.”

## Residual Limits

- The frame is GitHub-only and excludes closed-source usage.
- The study is intentionally 1-month fresh, which improves recency but narrows the frame relative to a 12-month ecosystem census.
- README fallback is still present for `41` final repos, so those stack edges should be read as low-confidence coverage rather than hard dependency evidence.
- `3` final repos still have unmapped dependency evidence and no normalized tracked edge.
- Benchmark thresholds now pass, but prioritized gap work remains, especially discovery coverage for `Daytona`, `DSPy`, and `TGI`.

## Publication Position

This snapshot is ready to publish as the project’s current public report.

The strongest one-line interpretation is:

> Major OSS AI repos are converging on multi-vendor, provider-centered, orchestration-heavy stacks, with retrieval now mainstream infrastructure and evaluation/observability still lagging behind.
