# Methodology Notes

This repository currently implements the first executable slice of the Phase 1 spec:

- candidate discovery from topic queries, description keyword queries, and manual seeds
- GraphQL metadata hydration for discovered repositories
- repository context collection using README, tree, and manifest files
- SBOM retrieval and direct-dependency parsing from GitHub dependency graph exports
- deterministic serious-project scoring
- deterministic AI relevance scoring
- manifest-based dependency extraction for Python, JavaScript/TypeScript, Go, and Rust
- bounded import-based fallback detection for Python, JavaScript/TypeScript, Go, and Rust
- normalized repo-to-technology edge generation for included repos

Current limitations:

- technology normalization is alias-based exact matching only
- reporting and publication stages are not implemented yet

Optional judge passes:

- a hardening pass can review borderline repos and optionally override the rule decision
- a validation pass can review already-selected final repos that have not been judged yet
- both paths are feature-flagged and off by default
- it requires `OPENAI_API_KEY`
