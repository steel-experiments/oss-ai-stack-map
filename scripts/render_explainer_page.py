from __future__ import annotations

import argparse
from datetime import datetime, timezone
from html import escape
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a static explainer page for the OSS AI Stack Map methodology."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/run-2026-03-25-repaired-v13"),
        help="Snapshot directory referenced by the explainer.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/explainer.html"),
        help="HTML file to write.",
    )
    return parser.parse_args()


def stat_card(label: str, value: str, note: str) -> str:
    return f"""
    <div class="border border-line bg-paper p-5">
      <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ {escape(label)}</div>
      <div class="mt-3 text-3xl font-semibold tracking-tight text-ink">{escape(value)}</div>
      <div class="mt-2 text-sm leading-6 text-muted">{escape(note)}</div>
    </div>
    """


def frame_cards(items: list[tuple[str, str]]) -> str:
    return "\n".join(
        f"""
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ frame</div>
          <h2 class="mt-3 text-xl font-semibold tracking-tight text-ink">{escape(title)}</h2>
          <p class="mt-4 text-sm leading-7 text-muted">{escape(body)}</p>
        </article>
        """
        for title, body in items
    )


def step_cards(items: list[str]) -> str:
    return "\n".join(
        f"""
        <article class="border border-line bg-paper p-5">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">step {index}</div>
          <div class="mt-3 text-sm leading-7 text-ink">{escape(item)}</div>
        </article>
        """
        for index, item in enumerate(items, start=1)
    )


def checklist(items: list[str]) -> str:
    return "\n".join(
        f"""
        <div class="border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="text-sm leading-7 text-ink">{escape(item)}</div>
        </div>
        """
        for item in items
    )


def faq_items(items: list[tuple[str, str]]) -> str:
    return "\n".join(
        f"""
        <details class="group border-t border-line py-4 first:border-t-0 first:pt-0">
          <summary class="cursor-pointer list-none pr-8 text-base font-medium leading-7 text-ink marker:hidden">
            {escape(question)}
          </summary>
          <div class="mt-3 max-w-3xl text-sm leading-7 text-muted">
            {escape(answer)}
          </div>
        </details>
        """
        for question, answer in items
    )


def build_page(input_dir: Path) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    build_id = input_dir.name

    frames = [
        (
            "What this map is",
            "A normalized graph of technologies used by serious OSS AI repos, not just a list of repositories.",
        ),
        (
            "What this map is not",
            "It is not a generic dependency census, unreviewed README keyword scrape, or popularity leaderboard.",
        ),
        (
            "Why repair-first matters",
            "Normalization and research quality improve faster than discovery, so we iterate by repairing snapshots against the current registry.",
        ),
    ]

    methodology_steps = [
        "Discover a broad GitHub universe from topics, description keywords, and manual seed repos.",
        "Build repo context from README, file tree, manifests, SBOMs, and imports.",
        "Classify which repos are serious, AI-relevant, and worth publishing in the final map.",
        "Extract dependency evidence and normalize it into canonical technologies and provider signals.",
        "Repair snapshots so new registry logic can be applied without doing a new GitHub crawl.",
        "Use gap reports, benchmarks, and graph analysis to decide what the registry should learn next.",
    ]

    evidence_sources = [
        "Manifest dependencies from Python, JavaScript/TypeScript, Go, and Rust manifests.",
        "SBOM direct dependencies from GitHub dependency graph exports.",
        "Import-derived fallback detection when manifest coverage is incomplete.",
        "Repo identity edges for upstream canonical projects.",
        "Curator-reviewed low-confidence README fallback only when a final repo would otherwise remain unmapped.",
    ]

    curation_rules = [
        "The canonical registry is the primary research surface. It tracks provider, product, sdk_family, package-family, aliases, package prefixes, repo identities, and capabilities.",
        "Auto-discovery finds and ranks candidate families, but does not promote them directly into the registry.",
        "Benchmarks measure recall against known AI stack entities so regressions become visible immediately.",
        "Optional LLM judges narrow review queues for repo classification and registry suggestions, but do not replace curation.",
    ]

    faqs = [
        (
            "Does discovery expand recursively through dependency graphs?",
            "Not today. Discovery starts from GitHub topics, description queries, and manual seed repos. Dependency evidence is collected later during classification and normalization, where it helps map technologies rather than recursively discover more repositories.",
        ),
        (
            "What is the judge actually doing?",
            "The judge is an optional reviewer for repo inclusion or registry suggestion triage. It is not the primary classifier and it does not define the technology map on its own. Its job is to narrow the queue of ambiguous cases.",
        ),
        (
            "What is the registry and why is it so important?",
            "The registry is the curated canonical list of technologies the project knows how to recognize. It stores aliases, package prefixes, import aliases, repo identities, category metadata, and entity type. Improving the registry is the fastest way to reduce unmapped evidence and missing edges.",
        ),
        (
            "Why does the project have a repair-first workflow?",
            "Normalization, registry curation, and reporting usually improve faster than GitHub discovery. Snapshot repair lets the project reapply better logic to an existing snapshot without paying for a new crawl every time.",
        ),
        (
            "What does the benchmark measure?",
            "The benchmark measures whether curated important entities are discovered, included, identity-mapped, observed in third-party adoption, and supported by dependency evidence. It is both a regression guard and a growing recall panel.",
        ),
        (
            "What counts as a good improvement?",
            "A good improvement raises benchmark recall, reduces final repos missing normalized edges, improves precision on hard cases, or simplifies the system without hurting the scorecard.",
        ),
        (
            "Can the registry suggestions be accepted automatically?",
            "No. Registry suggestions are ranked candidates inferred from real evidence, but they still require curator review. The project treats automatic candidate generation and curated promotion as separate steps.",
        ),
        (
            "What is the ideal final output of the project?",
            "The ideal output is a credible map of the enabling AI stack used across serious OSS AI projects: providers, model access layers, orchestration frameworks, runtimes, vector systems, training tools, observability systems, and other canonical technology entities.",
        ),
    ]

    mermaid_pipeline = """
flowchart LR
    A[GitHub Search + Manual Seeds] --> B[Discovery]
    B --> C[repos.parquet]
    C --> D[Classification Context Build]
    D --> E[repo_contexts.parquet]
    E --> F[Rule-Based Classification]
    F --> G[repo_inclusion_decisions.parquet]
    E --> H[Dependency Evidence]
    H --> I[repo_dependency_evidence.parquet]
    E --> J[Normalization]
    G --> J
    J --> K[repo_technology_edges.parquet]
    J --> L[technologies.parquet]
    C --> M[Snapshot Repair]
    E --> M
    G --> M
    K --> M
    L --> M
    M --> N[gap_report.json]
    M --> O[benchmark_recall_report.json]
    M --> P[technology_discovery_report.json]
    M --> Q[registry_suggestions.json]
    N --> R[Report + Explainer]
    O --> R
    P --> R
    Q --> R
""".strip()

    mermaid_lineage = """
flowchart TD
    A[repos.parquet] --> B[repo_contexts.parquet]
    B --> C[repo_dependency_evidence.parquet]
    B --> D[repo_inclusion_decisions.parquet]
    C --> E[repo_technology_edges.parquet]
    D --> E
    E --> F[technologies.parquet]
    A --> G[gap_report.json]
    C --> G
    D --> G
    E --> G
    A --> H[benchmark_recall_report.json]
    C --> H
    E --> H
    A --> I[technology_discovery_report.json]
    C --> I
    D --> I
    E --> I
    I --> J[registry_suggestions.json]
    H --> J
    J --> K[docs output]
""".strip()

    mermaid_registry = """
flowchart LR
    A[Unmatched dependency evidence] --> B[NetworkX candidate graph]
    B --> C[Ranked family backlog]
    C --> D[Registry suggestion filter]
    D --> E{LLM judge enabled?}
    E -- yes --> F[OpenAIRegistryJudge review]
    E -- no --> G[Curator review]
    F --> H[Accept / Review / Reject]
    G --> H
    H --> I[technology_registry.yaml]
    I --> J[snapshot-repair]
    J --> K[Improved edges, gaps, and benchmarks]
""".strip()

    mermaid_autoresearch = """
flowchart LR
    A[Discovery Queries and Manual Seeds] --> B[Candidate Repo Universe]
    B --> C[Rule Classifier]
    C --> D{Optional Judge}
    D --> E[Final Repo Set]
    E --> F[Dependency Evidence and Normalization]
    F --> G[Canonical Technology Map]
    G --> H[Gap Report + Benchmark Recall + Registry Suggestions]
    H --> I{Which lever is weak?}
    I --> J[Discovery Updates]
    I --> K[Classifier Updates]
    I --> L[Judge Prompt and Routing Updates]
    I --> M[Registry Updates]
    J --> A
    K --> C
    L --> D
    M --> F
""".strip()

    html = f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>OSS AI Stack Map Explainer</title>
    <meta
      name="description"
      content="Explainer for the OSS AI Stack Map methodology, normalization pipeline, and architecture."
    />
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Instrument+Sans:wght@400;500;600;700&display=swap"
      rel="stylesheet"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
      tailwind.config = {{
        theme: {{
          extend: {{
            colors: {{
              cloud: '#F0EEE9',
              paper: '#FBFAF7',
              ink: '#1A1816',
              line: '#CBC3B7',
              muted: '#766E64',
              'muted-strong': '#A89E91',
              'tropic-coral': '#E78B72',
              'tropic-papaya': '#E5A36F',
              'tropic-sun': '#D8BF6A',
              'tropic-kiwi': '#9EAF73',
              'tropic-seafoam': '#9EC3B5',
              'tropic-lagoon': '#6FA7A3',
              'tropic-flamingo': '#D7A0AA',
            }},
            fontFamily: {{
              sans: ['Instrument Sans', 'sans-serif'],
              mono: ['IBM Plex Mono', 'monospace'],
            }},
          }},
        }},
      }};
    </script>
    <script type="module">
      import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
      mermaid.initialize({{
        startOnLoad: true,
        theme: 'neutral',
        securityLevel: 'loose',
      }});
    </script>
  </head>
  <body class="min-h-screen overflow-x-hidden bg-cloud font-sans text-ink antialiased">
    <main class="mx-auto max-w-7xl px-6 py-8 sm:px-8 lg:px-10">
      <nav class="mb-8 flex flex-col gap-4 border border-line bg-paper px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">OSS AI Stack Map</div>
          <div class="mt-1 text-sm text-muted">Methodology, normalization model, and system architecture.</div>
        </div>
        <div class="flex flex-wrap gap-3">
          <a href="index.html" class="border border-line bg-cloud px-4 py-2 text-sm font-medium text-ink">Report</a>
          <a href="explainer.html" class="border border-ink bg-ink px-4 py-2 text-sm font-medium text-paper">Explainer</a>
        </div>
      </nav>

      <section class="overflow-hidden border border-line bg-ink px-6 py-8 text-paper sm:px-8 lg:px-10">
        <div class="grid gap-10 lg:grid-cols-[minmax(0,1.35fr)_22rem]">
          <div>
            <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[explainer]</div>
            <h1 class="mt-5 max-w-4xl text-4xl font-semibold tracking-[-0.04em] text-white sm:text-5xl lg:text-6xl">
              How this map is built and why the results are credible.
            </h1>
            <p class="mt-6 max-w-3xl text-base leading-8 text-paper/80 sm:text-lg">
              This page explains the exact method behind the OSS AI Stack Map: how repositories are discovered,
              how technology evidence is extracted, how raw package signals become canonical technologies, and how
              benchmarks plus graph analysis drive the next round of research.
            </p>
            <div class="mt-6 flex flex-wrap gap-3">
              <a href="index.html" class="border border-white/20 px-4 py-2 text-sm font-medium text-paper">Open the latest report</a>
              <a href="#method" class="border border-white/20 px-4 py-2 text-sm font-medium text-paper">Jump to method</a>
            </div>
          </div>

          <aside class="border border-white/10 p-6">
            <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[reference snapshot]</div>
            <div class="mt-4 text-2xl font-semibold tracking-tight text-paper">{escape(build_id)}</div>
            <div class="mt-4 text-sm leading-7 text-paper/80">
              Generated {escape(generated_at)} and aligned to <span class="break-all font-mono text-paper">{escape(str(input_dir))}</span>.
            </div>
          </aside>
        </div>
      </section>

      <section class="mt-8 border border-line bg-paper px-5 py-4">
        <div class="flex flex-wrap gap-3 text-sm">
          <a href="#overview" class="border border-line bg-cloud px-3 py-2 text-ink">Overview</a>
          <a href="#method" class="border border-line bg-cloud px-3 py-2 text-ink">Method</a>
          <a href="#evidence" class="border border-line bg-cloud px-3 py-2 text-ink">Evidence</a>
          <a href="#architecture" class="border border-line bg-cloud px-3 py-2 text-ink">Architecture</a>
          <a href="#operations" class="border border-line bg-cloud px-3 py-2 text-ink">Operations</a>
          <a href="#faq" class="border border-line bg-cloud px-3 py-2 text-ink">FAQ</a>
        </div>
      </section>

      <section id="overview" class="mt-8 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {stat_card("Research object", "AI tech stacks", "The unit of study is how major OSS AI repos compose their stack, not just which repos exist.")}
        {stat_card("Primary surface", "Registry", "technology_registry.yaml is the main curated research asset for canonical technologies.")}
        {stat_card("Iteration mode", "Repair-first", "We improve normalization and reporting by repairing snapshots without requiring a new GitHub crawl every time.")}
        {stat_card("Discovery mode", "Graph-guided", "Network analysis ranks unmatched package families so research effort follows the data.")}
      </section>

      <section class="mt-8 grid gap-6 lg:grid-cols-3">
        {frame_cards(frames)}
      </section>

      <section id="method" class="mt-14">
        <div class="max-w-3xl">
          <div class="font-mono text-[11px] uppercase tracking-[0.24em] text-muted">+ method</div>
          <h2 class="mt-3 text-3xl font-semibold tracking-[-0.03em] text-ink">The workflow in six concrete steps</h2>
        </div>
        <div class="mt-8 grid gap-5 lg:grid-cols-3">
          {step_cards(methodology_steps)}
        </div>
      </section>

      <section id="evidence" class="mt-14 grid gap-6 lg:grid-cols-2">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ evidence</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">What counts as stack evidence</h2>
          <div class="mt-6">
            {checklist(evidence_sources)}
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ curation</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">What stays automatic and what stays curated</h2>
          <div class="mt-6">
            {checklist(curation_rules)}
          </div>
          <div class="mt-6 border border-line bg-cloud p-5 text-sm leading-7 text-muted">
            <p><span class="font-medium text-ink">Matching order:</span> exact alias, registry alias, package-family prefix, provider inference, repo identity, then low-confidence README fallback.</p>
            <p class="mt-3"><span class="font-medium text-ink">Judge usage:</span> repo judges and registry judges are optional, conservative, and used to narrow review queues rather than replace curation.</p>
          </div>
        </article>
      </section>

      <section id="architecture" class="mt-14">
        <div class="max-w-3xl">
          <div class="font-mono text-[11px] uppercase tracking-[0.24em] text-muted">+ architecture</div>
          <h2 class="mt-3 text-3xl font-semibold tracking-[-0.03em] text-ink">The pipeline and the feedback loops that improve it</h2>
        </div>

        <article class="mt-8 border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[top-level flow]</div>
          <div class="mt-6 overflow-x-auto">
            <pre class="mermaid min-w-[48rem] bg-cloud p-4 font-mono text-sm">{escape(mermaid_pipeline)}</pre>
          </div>
        </article>

        <article class="mt-6 border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[artifact lineage]</div>
          <div class="mt-6 overflow-x-auto">
            <pre class="mermaid min-w-[48rem] bg-cloud p-4 font-mono text-sm">{escape(mermaid_lineage)}</pre>
          </div>
        </article>

        <article class="mt-6 border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[improvement loop]</div>
          <div class="mt-6 overflow-x-auto">
            <pre class="mermaid min-w-[48rem] bg-cloud p-4 font-mono text-sm">{escape(mermaid_autoresearch)}</pre>
          </div>
        </article>

        <article class="mt-6 border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[registry curation loop]</div>
          <div class="mt-6 overflow-x-auto">
            <pre class="mermaid min-w-[48rem] bg-cloud p-4 font-mono text-sm">{escape(mermaid_registry)}</pre>
          </div>
        </article>
      </section>

      <section id="operations" class="mt-14 grid gap-6 lg:grid-cols-2">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ operating model</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">How to work with the system</h2>
          <div class="mt-6 space-y-4 text-sm leading-7 text-muted">
            <p><span class="font-medium text-ink">Fresh universe:</span> use `discover` and `classify` when you need a new GitHub crawl.</p>
            <p><span class="font-medium text-ink">Research iteration:</span> use `snapshot-repair` when the registry, normalization rules, or reporting logic improves faster than discovery.</p>
            <p><span class="font-medium text-ink">Publication:</span> render the report to `docs/`, serve the directory over Tailscale, and keep the stable entrypoints at `index.html`, `oss-ai-stack-report-latest.html`, and `report-latest.json`.</p>
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ implementation map</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Where the code and config live</h2>
          <div class="mt-6 space-y-4 text-sm leading-7 text-muted">
            <p><span class="font-medium text-ink">Config:</span> `config/study_config.yaml`, `config/discovery_topics.yaml`, `config/technology_aliases.yaml`, `config/technology_registry.yaml`, `config/benchmark_entities.yaml`, `config/segment_rules.yaml`</p>
            <p><span class="font-medium text-ink">Pipeline:</span> `discovery.py`, `classification.py`, `normalize.py`, `reporting.py`, `technology_discovery.py`, `registry_suggestions.py`</p>
            <p><span class="font-medium text-ink">OpenAI review:</span> `openai/judge.py` and `openai/registry_judge.py`</p>
            <p><span class="font-medium text-ink">Docs and publication:</span> `analysis/snapshot.py`, `scripts/render_html_report.py`, `scripts/render_explainer_page.py`, `ARCHITECTURE.md`</p>
          </div>
        </article>
      </section>

      <section id="faq" class="mt-14">
        <article class="border border-line bg-paper p-6 sm:p-8">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ faq</div>
          <h2 class="mt-3 text-3xl font-semibold tracking-[-0.03em] text-ink">Common questions about the project</h2>
          <div class="mt-8">
            {faq_items(faqs)}
          </div>
        </article>
      </section>

      <footer class="mt-14 border border-line bg-paper px-6 py-5 text-sm leading-7 text-muted">
        <span class="font-mono text-ink">[source]</span> this explainer is generated from the current project architecture and methodology, and paired with the latest validated snapshot under <span class="break-all font-mono text-ink">{escape(str(input_dir))}</span>.
      </footer>
    </main>
  </body>
</html>
"""
    return html


def main() -> None:
    args = parse_args()
    html = build_page(args.input_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(f"Wrote explainer page to {args.output}")


if __name__ == "__main__":
    main()
