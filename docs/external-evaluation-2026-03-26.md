# External Evaluation

Date: `2026-03-26`

Evaluator stance: external methodology review of the live OSS AI Stack Map artifacts, code, and bundled snapshots.

Primary evaluated live artifact:

- stable entrypoint in [docs/index.html](/home/agent/oss-ai-stack-map/docs/index.html)
- live report target from [docs/report-latest.json](/home/agent/oss-ai-stack-map/docs/report-latest.json)

Supporting materials reviewed:

- [ARCHITECTURE.md](/home/agent/oss-ai-stack-map/ARCHITECTURE.md)
- [docs/methodology.md](/home/agent/oss-ai-stack-map/docs/methodology.md)
- [src/oss_ai_stack_map/pipeline/classification.py](/home/agent/oss-ai-stack-map/src/oss_ai_stack_map/pipeline/classification.py)
- [src/oss_ai_stack_map/pipeline/normalize.py](/home/agent/oss-ai-stack-map/src/oss_ai_stack_map/pipeline/normalize.py)
- [src/oss_ai_stack_map/pipeline/reporting.py](/home/agent/oss-ai-stack-map/src/oss_ai_stack_map/pipeline/reporting.py)
- [src/oss_ai_stack_map/analysis/snapshot.py](/home/agent/oss-ai-stack-map/src/oss_ai_stack_map/analysis/snapshot.py)
- [scripts/render_html_report.py](/home/agent/oss-ai-stack-map/scripts/render_html_report.py)

## Scorecard

| Dimension | Score / 10 | Verdict |
| --- | --- | --- |
| Reproducibility | 8 | Strong |
| Pipeline transparency | 8 | Strong |
| Measurement validity | 5 | Mixed |
| Recall coverage | 5 | Mixed |
| Precision / false-positive control | 6 | Moderate |
| Provenance and reporting hygiene | 4 | Weak |
| Publication readiness | 5 | Mixed |
| Overall | 6 | Useful research prototype, not yet a fully trustworthy public benchmark artifact |

## What Is Strong

The project is notably inspectable. Discovery, context collection, classification, normalization, reporting, snapshot repair, and validation are implemented as explicit code and materialized tables rather than hidden notebook logic. That is uncommon and valuable.

The pipeline is also operationally reproducible:

- outputs are checkpointed and versioned
- snapshots retain raw and repaired artifacts
- validation, gap reporting, benchmark recall, and registry suggestions exist as first-class outputs
- the bundled test suite passes cleanly

Methodologically, the best design decision is that the project prefers direct evidence over loose topical association. Manifests, SBOMs, imports, and canonical repo identities are much more defensible than pure README scraping.

## Main Weaknesses

The live artifact has been overstating determinism and understating judge influence. Review provenance existed in the data, but the published report was previously computing applied-judge counts from a dead field rather than the actual decision rows.

The map still relies on weak fallback evidence for a non-trivial tail of repos. In the live `v14` snapshot reviewed during this evaluation, `55` final repos mapped only through README fallback edges. That is acceptable as a coverage backstop, but not as evidence of stack adoption in the same sense as manifest or SBOM dependencies.

The judge had also been allowed to write free-form segment labels outside the configured taxonomy. That undermines comparability of segment distributions across runs and makes the segmentation layer less suitable for longitudinal analysis.

Finally, the benchmark set is still too small to support strong ecosystem-wide recall claims. It works as a regression guard, not as a comprehensive coverage argument.

## Live Snapshot Assessment

At review time, the stable report pointed to `data/run-2026-03-25-repaired-v14`.

Observed issues in that artifact:

- validation status was `error`
- `repo_technology_edges` contained repos outside the final included set
- the report page showed zero applied judges even though decision-level provenance showed substantial judge activity
- some summary figures were therefore unreliable as published

This is a reporting and artifact-lineage problem more than a core pipeline problem, but from an external evaluator's perspective the distinction does not matter much. The public artifact is what gets judged.

## Recommended Standard For Publication

The project is ready to be presented as:

- a serious exploratory research pipeline
- a transparent OSS mapping workflow
- a reproducible internal benchmarking artifact

It is not yet ready to be presented as:

- a stable public benchmark of the OSS AI ecosystem
- a precise stack-adoption census
- a longitudinal trend artifact without stronger taxonomy and provenance controls

## Required Improvements Before Stronger Claims

1. Publish only snapshots that pass validation.
2. Distinguish direct dependency edges from README fallback edges everywhere the graph is summarized.
3. Keep judge provenance explicit:
   reviewed count, override count, and whether the override changed inclusion, segment, or both.
4. Constrain segment outputs to a fixed taxonomy before publication.
5. Expand the benchmark entity set enough that recall claims become meaningful.
6. Keep a single up-to-date public methodology document and retire stale descriptions immediately.

## Bottom Line

This repository is better than most one-off research maps on engineering rigor and inspectability.

The current limitation is not lack of machinery. It is trust calibration. The public artifact needs to describe exactly how much is rule-based, how much is judge-adjusted, how much is direct dependency evidence, and how much is fallback inference. Once those boundaries are explicit and enforced, the project becomes substantially more credible.
