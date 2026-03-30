# Methodology

This project builds a reproducible snapshot of major open source AI repositories on GitHub, then maps the technologies those repositories appear to use.

## Current Pipeline

1. Discovery
   Starts from GitHub topic queries, description queries, and manual seed repositories.
2. Metadata hydration
   Normalizes repository metadata through GitHub GraphQL and REST APIs.
3. Context collection
   Collects README text, repository tree paths, manifest files, GitHub SBOM output, and bounded import evidence.
4. Classification
   Scores repositories on two dimensions:
   - serious project vs. tutorial, demo, list, or educational material
   - AI relevance based on direct dependency evidence plus limited structural/config signals
5. Normalization
   Converts raw package and import evidence into canonical technologies and provider signals.
6. Reporting and repair
   Produces summary tables, evidence-tier reports, validation-audit reports, gap reports, benchmark recall/precision reports, technology-discovery candidates, robustness checks, and HTML reports. Existing snapshots can be repaired against newer registry/config logic without rerunning GitHub discovery.

## Inclusion Logic

Hard scope filters are applied during discovery:

- public
- non-fork
- non-archived
- non-template
- pushed within the last 1 month
- above the configured star threshold

The classifier is rule-first:

- `serious_pass_score` and `ai_relevance_pass_score` are configured in [study_config.yaml](/home/agent/oss-ai-stack-map/config/study_config.yaml)
- hard exclusion keywords suppress tutorial, list, prompt-collection, and similar repos unless strong structural signals justify a narrow override
- final inclusion requires seriousness, AI relevance, and the major-repo star threshold

## Evidence Sources

The map prefers direct evidence in this order:

1. Manifest dependencies
2. GitHub SBOM dependencies
3. Import-derived matches
4. Repo identity matches for canonical upstream projects
5. Curator-reviewed low-confidence README fallback only when an included repo would otherwise have no normalized edge

Published evidence tiers are explicit:

- `direct_only`
  manifest, SBOM, import, and repo-identity evidence only
- `reviewed_fallback`
  direct evidence plus curator-reviewed README fallback for otherwise unmapped included repos
- `full_final_population`
  all included repos, including any remaining unmapped finals

Current structured manifest coverage is strongest for:

- Python
- JavaScript / TypeScript
- Go
- Rust

Some ecosystems and package formats remain under-covered.

## Normalization

Normalization is no longer exact-alias only.

The current matcher uses:

1. curated exact aliases
2. registry aliases
3. registry package-prefix rules
4. provider inference from package naming
5. repo identity mapping

The canonical research surface is [technology_registry.yaml](/home/agent/oss-ai-stack-map/config/technology_registry.yaml), with legacy alias support still present in [technology_aliases.yaml](/home/agent/oss-ai-stack-map/config/technology_aliases.yaml).

## LLM Judge Usage

LLM review is optional and disabled by default.

Two judge modes exist:

- `hardening`
  Reviews borderline cases and may override rule decisions when confidence meets the configured threshold.
- `validation`
  Reviews a seeded, stratified sample of repos already in the final set and can remove false positives.

The validation audit intentionally covers:

- primary-segment spread
- score-band spread
- README-only fallback repos
- repos whose current final-set status already depends on a prior judge override

The judge is not the primary classifier. It is a review layer on top of the rule system.

## Quality Control

Each finished snapshot can include:

- validation reports
- validation audit reports with sample composition and estimated false-positive rate
- gap reports for missing and unmapped edges
- benchmark recall against a curated positive panel plus holdout entities and exact negative controls
- technology-discovery candidates from unmatched evidence
- robustness checks comparing rule-only vs. judge-adjusted and direct-only vs. fallback-enabled views
- repaired snapshots with manifest and file lineage
- `review_queue.json` for README-only finals, audit-changed repos, benchmark gaps, and remaining missing-edge finals

## Important Limitations

- discovery is GitHub-query-driven, so recall depends on search terms and manual seeds
- benchmark coverage is broader than the initial seed set but still not exhaustive relative to the full AI OSS ecosystem
- README fallback edges improve coverage but are materially weaker than manifest, SBOM, import, or repo-identity evidence and should be treated as reviewed fallback coverage
- segment labels should be treated as stable only when they stay inside the configured taxonomy
- repaired snapshots are only as trustworthy as their preserved provenance and validation status
