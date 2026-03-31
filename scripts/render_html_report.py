from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from shutil import copyfile

import networkx as nx
import pyarrow.parquet as pq

CATEGORY_LABELS = {
    "model_access_and_providers": "Providers and access",
    "orchestration_and_agents": "Orchestration and agents",
    "training_finetuning_and_model_ops": "Model frameworks and HF stack",
    "vector_and_knowledge_storage": "Retrieval and vector storage",
    "serving_inference_and_local_runtimes": "Serving and local runtimes",
    "ui_and_app_frameworks": "UI and app frameworks",
    "observability_tracing_and_monitoring": "Observability",
    "evaluation_guardrails_and_safety": "Evaluation and guardrails",
    "ai_developer_tools_and_sdk_families": "Protocols and developer SDKs",
    "sandbox_and_isolated_execution": "Sandbox and isolated execution",
    "browser_and_computer_use_infra": "Browser and computer use infra",
    "runtime_and_agent_deployment": "Runtime and agent deployment",
}

SEGMENT_LABELS = {
    "serving_runtime": "Serving runtime",
    "training_finetuning": "Training and finetuning",
    "orchestration_framework": "Orchestration framework",
    "vector_retrieval_infrastructure": "Vector and retrieval infra",
    "agent_application": "Agent application",
    "rag_search_application": "RAG and search app",
    "eval_guardrails_observability": "Eval, guardrails, observability",
    "ai_application": "AI application",
    "ai_developer_tool": "AI developer tool",
    "general_builder_platform": "Builder platform",
    "unassigned": "Unassigned",
}

SECTION_ACCENTS = {
    "model_access_and_providers": "from-tropic-lagoon to-tropic-coral",
    "orchestration_and_agents": "from-tropic-flamingo to-tropic-papaya",
    "training_finetuning_and_model_ops": "from-tropic-kiwi to-tropic-lagoon",
    "vector_and_knowledge_storage": "from-tropic-lagoon to-tropic-seafoam",
    "serving_inference_and_local_runtimes": "from-tropic-sun to-tropic-coral",
    "ui_and_app_frameworks": "from-tropic-flamingo to-tropic-lagoon",
    "observability_tracing_and_monitoring": "from-tropic-lagoon to-tropic-sun",
    "evaluation_guardrails_and_safety": "from-tropic-kiwi to-tropic-flamingo",
}

DISPLAY_ROLLUPS = (
    {
        "family_id": "langchain-ecosystem",
        "label": "LangChain ecosystem",
        "member_ids": (
            "langchain",
            "langchain-openai",
            "langchain-anthropic",
            "langchain-google-genai",
            "langgraph",
        ),
    },
)

SVG_COLORS = {
    "ink": "#1A1816",
    "line": "#CBC3B7",
    "muted": "#766E64",
    "paper": "#FBFAF7",
    "cloud": "#F0EEE9",
    "coral": "#E78B72",
    "papaya": "#E5A36F",
    "sun": "#D8BF6A",
    "kiwi": "#9EAF73",
    "seafoam": "#9EC3B5",
    "lagoon": "#6FA7A3",
    "flamingo": "#D7A0AA",
}

CATEGORY_SVG_COLORS = {
    "model_access_and_providers": SVG_COLORS["coral"],
    "orchestration_and_agents": SVG_COLORS["flamingo"],
    "training_finetuning_and_model_ops": SVG_COLORS["kiwi"],
    "vector_and_knowledge_storage": SVG_COLORS["lagoon"],
    "serving_inference_and_local_runtimes": SVG_COLORS["papaya"],
    "ui_and_app_frameworks": SVG_COLORS["seafoam"],
    "observability_tracing_and_monitoring": SVG_COLORS["sun"],
    "evaluation_guardrails_and_safety": SVG_COLORS["coral"],
}

CATEGORY_SHORT_LABELS = {
    "model_access_and_providers": "Providers",
    "orchestration_and_agents": "Orchestration",
    "training_finetuning_and_model_ops": "Training",
    "vector_and_knowledge_storage": "Retrieval",
    "serving_inference_and_local_runtimes": "Serving",
    "ui_and_app_frameworks": "UI",
    "observability_tracing_and_monitoring": "Observability",
    "evaluation_guardrails_and_safety": "Eval",
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a static Tailwind HTML report from a staged oss-ai-stack-map snapshot."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/run-2026-03-25-resumable"),
        help="Directory containing staged parquet outputs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/oss-ai-stack-report-2026-03-25.html"),
        help="HTML file to write.",
    )
    return parser.parse_args()


def load_rows(path: Path, columns: list[str] | None = None) -> list[dict]:
    return pq.read_table(path, columns=columns).to_pylist()


def load_rows_if_exists(path: Path, columns: list[str] | None = None) -> list[dict]:
    if not path.exists():
        return []
    return load_rows(path, columns=columns)


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def fmt_int(value: int | float) -> str:
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.1f}"
    return f"{int(round(value)):,}"


def pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100


def pct_text(numerator: int, denominator: int) -> str:
    return f"{pct(numerator, denominator):.1f}%"


def quantile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    index = (len(values) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    fraction = index - lower
    return values[lower] + (values[upper] - values[lower]) * fraction


def humanize_segment(segment: str) -> str:
    return SEGMENT_LABELS.get(segment, segment.replace("_", " ").strip().title())


def tech_bar_rows(rows: list[dict], denominator: int, accent: str) -> str:
    parts: list[str] = []
    for row in rows:
        label = escape(str(row["label"]))
        note = escape(str(row.get("note", ""))).strip()
        count = int(row["count"])
        share = pct(count, denominator)
        parts.append(
            f"""
            <div class="space-y-2">
              <div class="flex flex-col gap-1 text-sm sm:flex-row sm:items-end sm:justify-between sm:gap-4">
                <div class="min-w-0">
                  <div class="break-words font-medium text-paper">{label}</div>
                  {'<div class="mt-1 break-words text-xs text-muted-strong">' + note + '</div>' if note else ''}
                </div>
                <div class="shrink-0 font-mono text-xs text-muted-strong sm:text-right">{fmt_int(count)} <span class="text-muted">/ {share:.1f}%</span></div>
              </div>
              <div class="h-2 overflow-hidden bg-white/8">
                <div class="h-full bg-gradient-to-r {accent}" style="width: {min(share, 100):.1f}%"></div>
              </div>
            </div>
            """
        )
    return "\n".join(parts)


def stat_card(label: str, value: str, note: str) -> str:
    return f"""
    <div class="border border-line bg-paper p-5">
      <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ {escape(label)}</div>
      <div class="mt-3 text-3xl font-semibold tracking-tight text-ink">{escape(value)}</div>
      <div class="mt-2 text-sm leading-6 text-muted">{escape(note)}</div>
    </div>
    """


def list_rows(rows: list[tuple[str, str]]) -> str:
    return "\n".join(
        f"""
        <div class="flex flex-col gap-1 border-t border-white/10 py-3 first:border-t-0 first:pt-0 last:pb-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
          <div class="text-sm text-paper">{escape(label)}</div>
          <div class="break-all font-mono text-xs text-muted-strong sm:max-w-[11rem] sm:text-right">{escape(value)}</div>
        </div>
        """
        for label, value in rows
    )


def pill_rows(rows: list[tuple[str, str]]) -> str:
    return "\n".join(
        f"""
        <div class="flex items-center justify-between gap-4 border-t border-white/10 py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0 text-sm text-paper">{escape(label)}</div>
          <div class="shrink-0 border border-white/10 px-2 py-1 font-mono text-[11px] text-muted-strong">{escape(value)}</div>
        </div>
        """
        for label, value in rows
    )


def build_display_rows(
    *,
    technology_ids: list[str],
    technology_counts: Counter[str],
    technology_repo_ids: dict[str, set[int]],
    technologies: dict[str, dict],
) -> list[dict]:
    rows: list[dict] = []
    consumed: set[str] = set()

    for rollup in DISPLAY_ROLLUPS:
        member_ids = [technology_id for technology_id in rollup["member_ids"] if technology_id in technology_ids]
        if not member_ids:
            continue
        repo_ids: set[int] = set()
        breakdown_parts: list[str] = []
        for technology_id in member_ids:
            repo_ids.update(technology_repo_ids.get(technology_id, set()))
            count = int(technology_counts.get(technology_id, 0))
            if count <= 0:
                continue
            breakdown_parts.append(
                f"{technologies[technology_id]['display_name']} {fmt_int(count)}"
            )
        if not repo_ids:
            continue
        consumed.update(member_ids)
        rows.append(
            {
                "label": rollup["label"],
                "count": len(repo_ids),
                "note": ", ".join(breakdown_parts),
            }
        )

    for technology_id in technology_ids:
        if technology_id in consumed:
            continue
        rows.append(
            {
                "label": technologies[technology_id]["display_name"],
                "count": int(technology_counts[technology_id]),
            }
        )

    return sorted(rows, key=lambda row: (-int(row["count"]), str(row["label"]).casefold()))


def svg_text(text: str) -> str:
    return escape(text, quote=True)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def scale_linear(value: float, domain_min: float, domain_max: float, range_min: float, range_max: float) -> float:
    if abs(domain_max - domain_min) < 1e-9:
        return (range_min + range_max) / 2
    ratio = (value - domain_min) / (domain_max - domain_min)
    return range_min + ratio * (range_max - range_min)


def bucket_repo_degree(value: int) -> str:
    if value <= 5:
        return str(value)
    if value <= 7:
        return "6-7"
    if value <= 10:
        return "8-10"
    if value <= 15:
        return "11-15"
    return "16+"


def build_graphs(
    edges: list[dict],
    technologies: dict[str, dict],
) -> tuple[nx.Graph, nx.Graph, set[str], set[str]]:
    bipartite = nx.Graph()
    for row in edges:
        repo_node = f"repo:{row['repo_id']}"
        tech_node = f"tech:{row['technology_id']}"
        tech = technologies[row["technology_id"]]
        bipartite.add_node(
            repo_node,
            bipartite="repo",
            repo_id=row["repo_id"],
            full_name=row["full_name"],
        )
        bipartite.add_node(
            tech_node,
            bipartite="technology",
            technology_id=row["technology_id"],
            display_name=tech["display_name"],
            category_id=tech.get("category_id") or "other",
        )
        bipartite.add_edge(repo_node, tech_node)

    repo_nodes = {node for node, data in bipartite.nodes(data=True) if data["bipartite"] == "repo"}
    tech_nodes = set(bipartite.nodes()) - repo_nodes
    tech_projection = nx.bipartite.weighted_projected_graph(bipartite, tech_nodes)
    for _, _, data in tech_projection.edges(data=True):
        data["distance"] = 1 / data["weight"]
    return bipartite, tech_projection, repo_nodes, tech_nodes


def render_bar_chart_svg(rows: list[dict], *, value_key: str, color: str) -> str:
    width = 720
    row_height = 28
    top = 16
    left = 190
    right = 64
    bottom = 16
    height = top + bottom + max(len(rows), 1) * row_height
    chart_width = width - left - right
    max_value = max((float(row[value_key]) for row in rows), default=1.0)
    grid_values = [0.0, max_value / 3, 2 * max_value / 3, max_value]

    parts = [
        f'<svg viewBox="0 0 {width} {height}" class="w-full" role="img" aria-label="Bar chart">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SVG_COLORS["paper"]}" />',
    ]
    for grid_value in grid_values:
        x = left + (chart_width * grid_value / max_value if max_value else 0)
        parts.append(
            f'<line x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - bottom + 2}" stroke="{SVG_COLORS["line"]}" stroke-dasharray="3 4" />'
        )
        parts.append(
            f'<text x="{x:.1f}" y="10" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="middle">{svg_text(fmt_int(grid_value))}</text>'
        )

    for index, row in enumerate(rows):
        y = top + index * row_height
        bar_width = chart_width * float(row[value_key]) / max_value if max_value else 0
        label = row["label"]
        value = row[value_key]
        parts.append(
            f'<text x="{left - 10}" y="{y + 15}" fill="{SVG_COLORS["ink"]}" font-size="11" text-anchor="end">{svg_text(str(label))}</text>'
        )
        parts.append(
            f'<rect x="{left}" y="{y + 5}" width="{bar_width:.1f}" height="12" rx="3" fill="{color}" />'
        )
        parts.append(
            f'<text x="{left + bar_width + 8:.1f}" y="{y + 15}" fill="{SVG_COLORS["muted"]}" font-size="10">{svg_text(fmt_int(value))}</text>'
        )

    parts.append("</svg>")
    return "".join(parts)


def render_histogram_svg(counts: list[tuple[str, int]], *, color: str) -> str:
    width = 720
    height = 260
    left = 44
    right = 16
    top = 16
    bottom = 42
    chart_width = width - left - right
    chart_height = height - top - bottom
    max_count = max((count for _, count in counts), default=1)
    bar_width = chart_width / max(len(counts), 1)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" class="w-full" role="img" aria-label="Histogram">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SVG_COLORS["paper"]}" />',
        f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="{SVG_COLORS["line"]}" />',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="{SVG_COLORS["line"]}" />',
    ]
    for index, (label, count) in enumerate(counts):
        x = left + index * bar_width + 4
        h = chart_height * count / max_count if max_count else 0
        y = height - bottom - h
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(bar_width - 8, 6):.1f}" height="{h:.1f}" fill="{color}" rx="3" />'
        )
        parts.append(
            f'<text x="{x + (bar_width - 8) / 2:.1f}" y="{height - bottom + 16}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="middle">{svg_text(label)}</text>'
        )
    for tick in [0, max_count / 2, max_count]:
        y = height - bottom - (chart_height * tick / max_count if max_count else 0)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="{SVG_COLORS["line"]}" stroke-dasharray="3 4" />'
        )
        parts.append(
            f'<text x="{left - 6}" y="{y + 4:.1f}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="end">{svg_text(fmt_int(tick))}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def render_scatter_svg(points: list[dict]) -> str:
    width = 720
    height = 320
    left = 54
    right = 18
    top = 18
    bottom = 42
    chart_width = width - left - right
    chart_height = height - top - bottom
    max_x = max((point["x"] for point in points), default=1.0)
    max_y = max((point["y"] for point in points), default=1.0)
    max_size = max((point["size"] for point in points), default=1.0)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" class="w-full" role="img" aria-label="Scatter plot">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SVG_COLORS["paper"]}" />',
        f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="{SVG_COLORS["line"]}" />',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="{SVG_COLORS["line"]}" />',
    ]
    for tick in [0, max_x / 2, max_x]:
        x = left + (chart_width * tick / max_x if max_x else 0)
        parts.append(
            f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{height - bottom}" stroke="{SVG_COLORS["line"]}" stroke-dasharray="3 4" />'
        )
        parts.append(
            f'<text x="{x:.1f}" y="{height - bottom + 16}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="middle">{svg_text(fmt_int(tick))}</text>'
        )
    for tick in [0, max_y / 2, max_y]:
        y = height - bottom - (chart_height * tick / max_y if max_y else 0)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="{SVG_COLORS["line"]}" stroke-dasharray="3 4" />'
        )
        parts.append(
            f'<text x="{left - 6}" y="{y + 4:.1f}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="end">{tick:.3f}</text>'
        )

    label_candidates = sorted(points, key=lambda point: (point["y"], point["x"]), reverse=True)[:8]
    labeled = {point["label"] for point in label_candidates}
    for point in points:
        x = left + (chart_width * point["x"] / max_x if max_x else chart_width / 2)
        y = height - bottom - (chart_height * point["y"] / max_y if max_y else chart_height / 2)
        radius = 4 + (10 * point["size"] / max_size if max_size else 0)
        color = CATEGORY_SVG_COLORS.get(point["category_id"], SVG_COLORS["lagoon"])
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" fill="{color}" fill-opacity="0.72" stroke="{SVG_COLORS["paper"]}" stroke-width="1.5" />'
        )
        if point["label"] in labeled:
            parts.append(
                f'<text x="{x + radius + 4:.1f}" y="{y - radius - 2:.1f}" fill="{SVG_COLORS["ink"]}" font-size="10">{svg_text(point["label"])}</text>'
            )

    parts.append(
        f'<text x="{left + chart_width / 2:.1f}" y="{height - 8}" fill="{SVG_COLORS["muted"]}" font-size="11" text-anchor="middle">Repo count</text>'
    )
    parts.append(
        f'<text x="14" y="{top + chart_height / 2:.1f}" fill="{SVG_COLORS["muted"]}" font-size="11" text-anchor="middle" transform="rotate(-90 14 {top + chart_height / 2:.1f})">Betweenness centrality</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def render_heatmap_svg(labels: list[str], values: dict[tuple[str, str], int]) -> str:
    cell = 34
    left = 126
    top = 74
    right = 16
    bottom = 16
    size = len(labels)
    width = left + right + size * cell
    height = top + bottom + size * cell
    max_value = max(values.values(), default=1)

    def cell_color(value: int) -> str:
        if max_value <= 0:
            return SVG_COLORS["cloud"]
        mix = value / max_value
        green = int(238 - 90 * mix)
        blue = int(233 - 80 * mix)
        red = int(240 - 128 * mix)
        return f"rgb({red},{green},{blue})"

    parts = [
        f'<svg viewBox="0 0 {width} {height}" class="w-full" role="img" aria-label="Heatmap">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{SVG_COLORS["paper"]}" />',
    ]
    for col_index, label in enumerate(labels):
        x = left + col_index * cell + cell / 2
        parts.append(
            f'<text x="{x:.1f}" y="{top - 12}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="middle">{svg_text(CATEGORY_SHORT_LABELS.get(label, label[:10]))}</text>'
        )
    for row_index, left_label in enumerate(labels):
        y = top + row_index * cell + cell / 2 + 4
        parts.append(
            f'<text x="{left - 8}" y="{y:.1f}" fill="{SVG_COLORS["muted"]}" font-size="10" text-anchor="end">{svg_text(CATEGORY_SHORT_LABELS.get(left_label, left_label[:10]))}</text>'
        )
        for col_index, right_label in enumerate(labels):
            x = left + col_index * cell
            key = tuple(sorted((left_label, right_label)))
            value = values.get(key, 0)
            parts.append(
                f'<rect x="{x}" y="{top + row_index * cell}" width="{cell - 2}" height="{cell - 2}" fill="{cell_color(value)}" stroke="{SVG_COLORS["paper"]}" />'
            )
            if value:
                parts.append(
                    f'<text x="{x + cell / 2:.1f}" y="{top + row_index * cell + cell / 2 + 4:.1f}" fill="{SVG_COLORS["ink"]}" font-size="9" text-anchor="middle">{svg_text(fmt_int(value))}</text>'
                )
    parts.append("</svg>")
    return "".join(parts)


def render_inline_pills(values: list[str], *, tone: str = "cloud") -> str:
    bg_class = "bg-cloud" if tone == "cloud" else "bg-ink"
    text_class = "text-ink" if tone == "cloud" else "text-paper"
    border_class = "border-line" if tone == "cloud" else "border-white/10"
    return "".join(
        f'<span class="border {border_class} {bg_class} px-2 py-1 text-[11px] {text_class}">{escape(value)}</span>'
        for value in values
    )


def label_community(
    *,
    category_counter: Counter[str],
    technology_names: list[str],
) -> tuple[str, str]:
    top_categories = [category_id for category_id, _ in category_counter.most_common(3)]
    names = {name.casefold() for name in technology_names}

    if "steel browser" in names and len(technology_names) == 1:
        return (
            "Browser Infra Singleton",
            "A narrow browser-automation island that currently stays mostly separate from the rest of the mapped stack.",
        )
    if "transformers" in names or "pytorch" in names or "vllm" in names:
        return (
            "Training And Inference Core",
            "Model-training and inference-runtime technologies clustered around finetuning, serving, and heavyweight model execution.",
        )
    if "qdrant" in names or "weaviate" in names or "pgvector" in names or "milvus" in names:
        return (
            "Retrieval And App Surface",
            "Vector storage, retrieval plumbing, and lightweight app frameworks that often sit at the presentation edge of AI systems.",
        )
    if (
        "orchestration_and_agents" in top_categories
        or "model_access_and_providers" in top_categories
    ):
        return (
            "Provider And Orchestration Layer",
            "The dominant application-layer cluster where provider SDKs, orchestration frameworks, guardrails, and observability tools co-occur.",
        )
    return (
        "Mixed Stack Cluster",
        "A cross-linked family of technologies without a single dominant role.",
    )


def build_report(input_dir: Path) -> str:
    repos = load_rows(input_dir / "repos.parquet")
    decisions = load_rows(input_dir / "repo_inclusion_decisions.parquet")
    repo_contexts = load_rows(input_dir / "repo_contexts.parquet")
    all_edges = load_rows_if_exists(input_dir / "repo_technology_edges.parquet")
    technologies = {
        row["technology_id"]: row for row in load_rows(input_dir / "technologies.parquet")
    }
    gap_report = load_json(input_dir / "gap_report.json")
    benchmark_report = load_json(input_dir / "benchmark_recall_report.json")
    evidence_tier_report = load_json(input_dir / "evidence_tier_report.json")
    validation_audit_report = load_json(input_dir / "validation_audit_report.json")
    robustness_report = load_json(input_dir / "robustness_report.json")
    technology_discovery_report = load_json(input_dir / "technology_discovery_report.json")
    registry_suggestions_report = load_json(input_dir / "registry_suggestions.json")
    validation_sample_summary = load_json(input_dir / "validation_sample_summary.json")

    repo_by_id = {row["repo_id"]: row for row in repos}
    context_by_id = {row["repo_id"]: row for row in repo_contexts}
    final_decisions = [row for row in decisions if row["passed_major_filter"]]
    final_repo_ids = {row["repo_id"] for row in final_decisions}
    edges = [row for row in all_edges if row["repo_id"] in final_repo_ids]
    snapshot_date = str(repos[0]["snapshot_date"]) if repos else "unknown"

    total_repos = len(repos)
    serious_repos = sum(1 for row in decisions if row["passed_serious_filter"])
    ai_relevant_repos = sum(1 for row in decisions if row["passed_ai_relevance_filter"])
    final_repos = len(final_decisions)

    owner_final = Counter((repo_by_id[row["repo_id"]].get("owner_type") or "Unknown") for row in final_decisions)
    language_final = Counter(
        (repo_by_id[row["repo_id"]].get("primary_language") or "Unknown") for row in final_decisions
    )
    segment_counts = Counter((row.get("primary_segment") or "unassigned") for row in final_decisions)

    final_stars = sorted(repo_by_id[repo_id]["stars"] for repo_id in final_repo_ids)
    median_stars = quantile(final_stars, 0.50)
    p75_stars = quantile(final_stars, 0.75)

    final_with_manifest = sum(
        1 for repo_id in final_repo_ids if context_by_id[repo_id].get("manifest_paths")
    )
    final_with_sbom = sum(
        1 for repo_id in final_repo_ids if context_by_id[repo_id].get("sbom_dependencies")
    )

    unique_repo_tech_pairs: set[tuple[int, str]] = set()
    top_technologies: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    technology_repo_ids: defaultdict[str, set[int]] = defaultdict(set)
    repo_to_technologies: defaultdict[int, set[str]] = defaultdict(set)
    repo_to_edge_types: defaultdict[int, set[str]] = defaultdict(set)
    provider_pairs: set[tuple[int, str]] = set()
    provider_label_counts: defaultdict[str, Counter[str]] = defaultdict(Counter)
    category_top: defaultdict[str, Counter[str]] = defaultdict(Counter)
    category_repo_ids: defaultdict[str, set[int]] = defaultdict(set)

    for row in edges:
        pair = (row["repo_id"], row["technology_id"])
        if pair in unique_repo_tech_pairs:
            continue
        unique_repo_tech_pairs.add(pair)
        repo_to_technologies[row["repo_id"]].add(row["technology_id"])
        technology_repo_ids[row["technology_id"]].add(row["repo_id"])
        repo_to_edge_types[row["repo_id"]].add(row["evidence_type"])
        top_technologies[row["technology_id"]] += 1
        category_id = row.get("category_id") or "other"
        category_counts[category_id] += 1
        category_top[category_id][row["technology_id"]] += 1
        category_repo_ids[category_id].add(row["repo_id"])
        provider_id = row.get("provider_id")
        if provider_id:
            provider_pairs.add((row["repo_id"], provider_id))
            provider_label_counts[provider_id][technologies[row["technology_id"]]["display_name"]] += 1

    provider_counts: Counter[str] = Counter(provider_id for _, provider_id in provider_pairs)
    provider_labels = {
        provider_id: labels.most_common(1)[0][0] if labels else provider_id
        for provider_id, labels in provider_label_counts.items()
    }
    repos_with_edges = len(repo_to_technologies)
    repos_without_edges = final_repos - repos_with_edges
    readme_only_repo_count = sum(
        1 for repo_id in final_repo_ids if repo_to_edge_types.get(repo_id) == {"readme_mention"}
    )
    tech_count_distribution = sorted(len(values) for values in repo_to_technologies.values())
    median_techs = quantile(tech_count_distribution, 0.50)
    repo_to_providers: defaultdict[int, set[str]] = defaultdict(set)
    for repo_id, provider_id in provider_pairs:
        repo_to_providers[repo_id].add(provider_id)
    multi_provider_repo_count = sum(1 for provider_ids in repo_to_providers.values() if len(provider_ids) >= 2)
    provider_co_occurrence: Counter[tuple[str, str]] = Counter()
    for provider_ids in repo_to_providers.values():
        ordered = sorted(provider_ids)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                provider_co_occurrence[(left, right)] += 1
    strongest_provider_pairs = [
        {
            "left": provider_labels.get(left, left),
            "right": provider_labels.get(right, right),
            "count": count,
        }
        for (left, right), count in provider_co_occurrence.most_common(3)
    ]

    co_occurrence: Counter[tuple[str, str]] = Counter()
    for technology_ids in repo_to_technologies.values():
        ordered = sorted(technology_ids)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                co_occurrence[(left, right)] += 1

    strongest_pairs = [
        {
            "left": technologies[left]["display_name"],
            "right": technologies[right]["display_name"],
            "count": count,
        }
        for (left, right), count in co_occurrence.most_common(6)
    ]

    bipartite, tech_projection, repo_nodes, tech_nodes = build_graphs(edges, technologies)
    repo_degree_values = [bipartite.degree(node) for node in repo_nodes]
    degree_buckets = Counter(bucket_repo_degree(value) for value in repo_degree_values)
    degree_bucket_order = ["1", "2", "3", "4", "5", "6-7", "8-10", "11-15", "16+"]
    repo_degree_histogram = [(label, degree_buckets.get(label, 0)) for label in degree_bucket_order]

    if tech_projection.number_of_nodes():
        betweenness = nx.betweenness_centrality(tech_projection, weight="distance")
        eigenvector = nx.eigenvector_centrality(tech_projection, weight="weight", max_iter=5000)
        communities = list(nx.community.greedy_modularity_communities(tech_projection, weight="weight"))
        communities = sorted(communities, key=len, reverse=True)
        modularity = nx.community.modularity(tech_projection, communities, weight="weight")
    else:
        betweenness = {}
        eigenvector = {}
        communities = []
        modularity = 0.0

    eigenvector_rows = [
        {
            "label": tech_projection.nodes[node]["display_name"],
            "value": value,
        }
        for node, value in sorted(eigenvector.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    scatter_points = []
    for node in tech_projection.nodes():
        technology_id = tech_projection.nodes[node]["technology_id"]
        scatter_points.append(
            {
                "label": tech_projection.nodes[node]["display_name"],
                "x": top_technologies.get(technology_id, 0),
                "y": betweenness.get(node, 0.0),
                "size": sum(data["weight"] for _, _, data in tech_projection.edges(node, data=True)),
                "category_id": tech_projection.nodes[node]["category_id"],
            }
        )

    category_mixing = Counter()
    for left_node, right_node, data in tech_projection.edges(data=True):
        left_category = tech_projection.nodes[left_node]["category_id"]
        right_category = tech_projection.nodes[right_node]["category_id"]
        category_mixing[tuple(sorted((left_category, right_category)))] += data["weight"]
    heatmap_categories = [
        category_id
        for category_id, _ in category_counts.most_common(8)
    ]

    community_cards = []
    for index, community in enumerate(communities[:3], start=1):
        community_tech_ids = {
            tech_projection.nodes[node]["technology_id"]
            for node in community
        }
        community_tech_names = sorted(
            (technologies[technology_id]["display_name"] for technology_id in community_tech_ids),
            key=str.casefold,
        )
        category_counter = Counter(
            technologies[technology_id].get("category_id") or "other"
            for technology_id in community_tech_ids
        )
        exemplar_rows = []
        for repo_id, tech_ids in repo_to_technologies.items():
            overlap = len(tech_ids & community_tech_ids)
            if not overlap:
                continue
            exemplar_rows.append(
                (
                    overlap,
                    repo_by_id[repo_id]["stars"],
                    repo_by_id[repo_id]["full_name"],
                )
            )
        exemplar_rows.sort(reverse=True)
        exemplar_repos = [full_name for _, _, full_name in exemplar_rows[:5]]
        top_categories = [
            f"{CATEGORY_SHORT_LABELS.get(category_id, category_id)} ({count})"
            for category_id, count in category_counter.most_common(3)
        ]
        community_label, community_note = label_community(
            category_counter=category_counter,
            technology_names=community_tech_names,
        )
        community_cards.append(
            f"""
            <article class="border border-line bg-cloud p-5">
              <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[community {index}]</div>
                  <h3 class="mt-2 text-xl font-semibold tracking-tight text-ink">{escape(community_label)}</h3>
                  <div class="mt-1 text-sm leading-6 text-muted">{escape(community_note)}</div>
                </div>
                <div class="border border-line bg-paper px-3 py-2 font-mono text-[11px] text-muted">share {pct_text(len(community_tech_ids), len(tech_nodes) or 1)}</div>
              </div>
              <div class="mt-4 border border-line bg-paper px-3 py-2 font-mono text-[11px] text-muted">{fmt_int(len(community_tech_ids))} technologies</div>
              <div class="mt-4">
                <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">top categories</div>
                <div class="mt-2 flex flex-wrap gap-2">
                  {render_inline_pills(top_categories)}
                </div>
              </div>
              <div class="mt-4">
                <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">top technologies</div>
                <div class="mt-2 flex flex-wrap gap-2">
                  {render_inline_pills(community_tech_names[:6])}
                </div>
              </div>
              <div class="mt-4">
                <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">exemplar repos</div>
                <div class="mt-2 flex flex-wrap gap-2">
                  {render_inline_pills(exemplar_repos[:5])}
                </div>
              </div>
            </article>
            """
        )

    eigenvector_chart = render_bar_chart_svg(eigenvector_rows, value_key="value", color=SVG_COLORS["lagoon"])
    repo_degree_chart = render_histogram_svg(repo_degree_histogram, color=SVG_COLORS["papaya"])
    centrality_scatter = render_scatter_svg(scatter_points)
    category_heatmap = render_heatmap_svg(heatmap_categories, category_mixing)

    category_sections = []
    for category_id, count in category_counts.most_common():
        top_rows = build_display_rows(
            technology_ids=list(category_top[category_id]),
            technology_counts=category_top[category_id],
            technology_repo_ids=technology_repo_ids,
            technologies=technologies,
        )[:5]
        category_sections.append(
            {
                "category_id": category_id,
                "label": CATEGORY_LABELS.get(category_id, category_id.replace("_", " ").title()),
                "count": count,
                "share": pct(count, len(unique_repo_tech_pairs)),
                "repo_count": len(category_repo_ids[category_id]),
                "repo_share": pct(len(category_repo_ids[category_id]), final_repos),
                "technology_count": len(category_top[category_id]),
                "rows": top_rows,
            }
        )

    top_tech_rows = [
        row
        for row in build_display_rows(
            technology_ids=list(top_technologies),
            technology_counts=top_technologies,
            technology_repo_ids=technology_repo_ids,
            technologies=technologies,
        )[:10]
    ]
    top_provider_rows = [
        {"label": provider_labels.get(provider_id, provider_id), "count": count}
        for provider_id, count in provider_counts.most_common(3)
    ]
    top_segment_rows = [
        {"label": humanize_segment(segment), "count": count}
        for segment, count in segment_counts.most_common(6)
    ]
    owner_rows = [{"label": owner, "count": count} for owner, count in owner_final.most_common()]
    top_language_rows = [
        {"label": language, "count": count}
        for language, count in language_final.most_common(5)
    ]

    judge_reviewed_count = sum(1 for row in decisions if row.get("judge_applied"))
    judge_override_count = sum(1 for row in decisions if row.get("judge_override_applied"))
    run_state = json.loads((input_dir / "checkpoints" / "run_state.json").read_text())
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    gap_missing_edges_count = int((gap_report or {}).get("final_repos_missing_edges_count", repos_without_edges))
    gap_missing_edges_unmapped = int(
        (gap_report or {}).get("final_repos_missing_edges_with_unmapped_dependency_evidence_count", 0)
    )
    gap_missing_edges_no_dep = int(
        (gap_report or {}).get("final_repos_missing_edges_with_no_dependency_evidence_count", 0)
    )
    gap_unmatched_prefixes = (gap_report or {}).get("top_unmatched_package_prefixes", [])
    gap_ai_specific_prefixes = (gap_report or {}).get("top_ai_specific_unmatched_package_prefixes", [])
    gap_commodity_prefixes = (gap_report or {}).get("top_commodity_unmatched_package_prefixes", [])
    gap_vendor_like_unmapped = (gap_report or {}).get("top_vendor_like_unmapped_repos", [])
    gap_missing_edge_repos = (gap_report or {}).get("final_repos_missing_edges", [])
    benchmark_entities = int((benchmark_report or {}).get("entity_count", 0))
    benchmark_total_entities = int((benchmark_report or {}).get("total_entity_count", benchmark_entities))
    benchmark_negative_entities = int((benchmark_report or {}).get("negative_entity_count", 0))
    benchmark_holdout_entities = int((benchmark_report or {}).get("holdout_entity_count", 0))
    benchmark_failed_thresholds = (benchmark_report or {}).get("failed_thresholds", [])
    benchmark_prioritized_gaps = (benchmark_report or {}).get("prioritized_gaps", [])
    registry_suggestions = (registry_suggestions_report or {}).get("suggestions", [])
    build_id = input_dir.name
    evidence_direct_supported = int((evidence_tier_report or {}).get("direct_supported_repo_count", repos_with_edges - readme_only_repo_count))
    evidence_fallback_supported = int((evidence_tier_report or {}).get("fallback_supported_repo_count", repos_with_edges))
    evidence_unmapped = int((evidence_tier_report or {}).get("unmapped_final_repo_count", repos_without_edges))
    evidence_readme_only = int((evidence_tier_report or {}).get("readme_only_repo_count", readme_only_repo_count))
    evidence_repo_identity_only = int((evidence_tier_report or {}).get("repo_identity_only_repo_count", 0))
    evidence_mixed_fallback = int((evidence_tier_report or {}).get("mixed_fallback_repo_count", 0))
    validation_audit_sample_count = int((validation_audit_report or {}).get("sample_count", 0))
    validation_audit_changed_count = int((validation_audit_report or {}).get("changed_decision_count", 0))
    validation_audit_exclusion_count = int((validation_audit_report or {}).get("exclusion_count", 0))
    validation_audit_change_rate = float((validation_audit_report or {}).get("inclusion_change_rate", 0.0))
    validation_audit_false_positive_rate = float((validation_audit_report or {}).get("estimated_false_positive_rate", 0.0))
    validation_audit_false_positive_ci = (validation_audit_report or {}).get("estimated_false_positive_rate_ci95", {"lower": 0.0, "upper": 0.0})
    validation_audit_segments = (validation_audit_report or {}).get("segment_counts", [])
    robustness_rule_only = (robustness_report or {}).get("rule_only", {})
    robustness_judge_adjusted = (robustness_report or {}).get("judge_adjusted", {})
    robustness_temporal = (robustness_report or {}).get("temporal_comparison")

    benchmark_rows = []
    if benchmark_report:
        benchmark_rows = [
            ("Repo discovered", pct_text(benchmark_report["entities_with_repo_discovered"], benchmark_entities)),
            ("Repo included", pct_text(benchmark_report["entities_with_repo_included"], benchmark_entities)),
            ("Identity mapped", pct_text(benchmark_report["entities_with_repo_identity_mapped"], benchmark_entities)),
            ("Third-party adoption", pct_text(benchmark_report["entities_with_third_party_adoption"], benchmark_entities)),
            ("Dependency evidence", pct_text(benchmark_report["entities_with_dependency_evidence"], benchmark_entities)),
            (
                "Negative excluded",
                f"{100 * float(benchmark_report.get('negative_repo_excluded_rate', 0.0)):.1f}%"
                if benchmark_negative_entities
                else "n/a",
            ),
            (
                "Holdout discovered",
                f"{100 * float(benchmark_report.get('holdout_repo_discovered_rate', 0.0)):.1f}%"
                if benchmark_holdout_entities
                else "n/a",
            ),
        ]

    judge_summary = (
        f"{fmt_int(judge_reviewed_count)} repos were judge-reviewed and "
        f"{fmt_int(judge_override_count)} judge overrides were applied in this snapshot. "
    )
    judge_summary += (
        "The published set remains rule-first, but not purely rule-only."
        if judge_override_count
        else "The published set remains rule-driven."
    )
    if validation_sample_summary:
        sampled_candidates = int(validation_sample_summary.get("sampled_validation_candidates", 0))
        sampled_final_repos = int(validation_sample_summary.get("final_repo_count", final_repos))
        judge_summary += (
            f" A seeded validation sample reviewed {fmt_int(sampled_candidates)} final repos "
            f"({pct_text(sampled_candidates, sampled_final_repos)}) in addition to hardening."
        )

    gap_prefix_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0 break-all text-sm text-ink">{escape(str(row['package_prefix']))}</div>
          <div class="font-mono text-xs text-muted">{fmt_int(int(row['count']))}</div>
        </div>
        """
        for row in gap_ai_specific_prefixes[:6]
    ) or '<div class="text-sm text-muted">No AI-specific unmatched package prefixes in this snapshot.</div>'

    gap_commodity_prefix_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0 break-all text-sm text-ink">{escape(str(row['package_prefix']))}</div>
          <div class="font-mono text-xs text-muted">{fmt_int(int(row['count']))}</div>
        </div>
        """
        for row in gap_commodity_prefixes[:6]
    ) or '<div class="text-sm text-muted">No commodity/tooling prefix backlog in this snapshot.</div>'

    gap_repo_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
            <div class="min-w-0">
            <div class="break-all text-sm font-medium text-ink">{escape(str(row['full_name']))}</div>
            <div class="mt-1 text-xs text-muted">{escape(str(row.get('gap_reason', 'missing_edge')).replace('_', ' '))} • unmatched deps {fmt_int(int(row.get('unmatched_dependency_count', 0)))}</div>
          </div>
          <div class="font-mono text-xs text-muted">{fmt_int(int(row.get('stars', 0)))} stars</div>
        </div>
        """
        for row in gap_missing_edge_repos[:6]
    ) or '<div class="text-sm text-muted">No missing-edge repos in this snapshot.</div>'

    benchmark_gap_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0">
            <div class="break-words text-sm font-medium text-ink">{escape(str(row['display_name']))}</div>
            <div class="mt-1 text-xs text-muted">{escape('; '.join(row.get('reasons', [])))}</div>
          </div>
          <div class="font-mono text-xs text-muted">score {fmt_int(int(row['priority_score']))}</div>
        </div>
        """
        for row in benchmark_prioritized_gaps[:5]
    ) or '<div class="text-sm text-muted">No prioritized benchmark gaps in this snapshot.</div>'

    evidence_profile_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0 break-all text-sm text-ink">{escape(str(row['evidence_profile']).replace('_', ' '))}</div>
          <div class="font-mono text-xs text-muted">{fmt_int(int(row['repo_count']))}</div>
        </div>
        """
        for row in (evidence_tier_report or {}).get("repo_evidence_profiles", [])[:6]
    ) or '<div class="text-sm text-muted">No evidence-tier profile data in this snapshot.</div>'

    validation_segment_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0 break-all text-sm text-ink">{escape(humanize_segment(str(row['primary_segment'])))}</div>
          <div class="font-mono text-xs text-muted">{fmt_int(int(row['repo_count']))}</div>
        </div>
        """
        for row in validation_audit_segments[:6]
    ) or '<div class="text-sm text-muted">No validation audit segment data in this snapshot.</div>'

    technology_candidate_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0">
            <div class="break-words text-sm font-medium text-ink">{escape(str(row['suggested_display_name']))}</div>
            <div class="mt-1 text-xs text-muted">{escape(', '.join(row.get('suggested_package_prefixes', [])[:3]))}</div>
          </div>
          <div class="font-mono text-xs text-muted">score {row['priority_score']:.1f}</div>
        </div>
        """
        for row in registry_suggestions[:6]
    ) or '<div class="text-sm text-muted">No post-filtered canonical discovery candidates available for this snapshot.</div>'

    registry_suggestion_rows = "\n".join(
        f"""
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0">
          <div class="min-w-0">
            <div class="break-words text-sm font-medium text-ink">{escape(str(row['suggested_display_name']))}</div>
            <div class="mt-1 text-xs text-muted">{escape(', '.join(row.get('suggested_package_prefixes', [])[:3]))}</div>
          </div>
          <div class="font-mono text-xs text-muted">{escape(str(row['confidence']))} • {row['priority_score']:.1f}</div>
        </div>
        """
        for row in registry_suggestions[:6]
    ) or '<div class="text-sm text-muted">No registry suggestions available for this snapshot.</div>'

    provider_category_count = category_counts.get("model_access_and_providers", 0)
    top_provider_id, top_provider_count = next(iter(provider_counts.most_common(1)), ("provider", 0))
    top_provider_label = provider_labels.get(top_provider_id, top_provider_id)
    top_provider_pair = strongest_provider_pairs[0] if strongest_provider_pairs else {
        "left": "two providers",
        "right": "another",
        "count": 0,
    }
    second_provider_pair = strongest_provider_pairs[1] if len(strongest_provider_pairs) > 1 else None
    eval_count = category_counts.get("evaluation_guardrails_and_safety", 0)
    observability_count = category_counts.get("observability_tracing_and_monitoring", 0)

    findings = [
        (
            "Providers sit at the center",
            f"Provider SDKs account for {pct_text(provider_category_count, len(unique_repo_tech_pairs))} of all normalized edges, and {top_provider_label} appears in {fmt_int(top_provider_count)} final repos. The provider layer is the most common and the most connective part of the stack.",
        ),
        (
            "Multi-provider stacks are common",
            (
                f"{fmt_int(multi_provider_repo_count)} of {fmt_int(repos_with_edges)} technology-mapped repos "
                f"({pct_text(multi_provider_repo_count, repos_with_edges)}) use at least two tracked providers. "
                f"The most common provider pairing is {top_provider_pair['left']} plus {top_provider_pair['right']} in {fmt_int(int(top_provider_pair['count']))} repos."
                + (
                    f" That is followed by {second_provider_pair['left']} plus {second_provider_pair['right']} in {fmt_int(int(second_provider_pair['count']))} repos."
                    if second_provider_pair
                    else ""
                )
                + " Major OSS projects are not clustering around a single vendor."
            ),
        ),
        (
            "The ecosystem is backend-heavy",
            f"Training, orchestration, providers, and retrieval dominate the graph. Evaluation and observability remain comparatively thin, with only {fmt_int(eval_count)} guardrail/eval edges and {fmt_int(observability_count)} observability edges.",
        ),
    ]

    modal_stack_summary = (
        "Inference from the aggregate counts: the modal major OSS AI repo in this snapshot is "
        "organization-owned, Python-first, anchored on a provider SDK, often layers in Hugging Face "
        "training tools, and then adds orchestration, retrieval, and a lightweight UI shell."
    )

    ascii_banner = "\n".join(
        [
            "+--------------------------------------------------------------+",
            "| OSS AI STACK MAP :: SNAPSHOT REPORT                          |",
            f"| SOURCE: {str(input_dir):<52.52} |",
            "| LENS: major, active, public OSS AI repos on GitHub           |",
            "+--------------------------------------------------------------+",
        ]
    )

    finding_cards = "\n".join(
        f"""
        <article class="border border-line bg-ink p-6 text-paper">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[finding]</div>
          <h3 class="mt-3 text-xl font-semibold tracking-tight">{escape(title)}</h3>
          <p class="mt-3 text-sm leading-7 text-paper/80">{escape(body)}</p>
        </article>
        """
        for title, body in findings
    )

    primary_category_sections = [
        section
        for section in category_sections
        if section["technology_count"] >= 2 and section["count"] >= 10
    ]
    thin_category_sections = [
        section
        for section in category_sections
        if section not in primary_category_sections
    ]

    category_cards = "\n".join(
        f"""
        <article class="border border-line bg-ink">
          <div class="p-6">
            <div class="flex items-start justify-between gap-4">
              <div>
                <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">+ {escape(section['label'])}</div>
                <div class="mt-2 text-2xl font-semibold tracking-tight text-paper">{fmt_int(section['count'])}</div>
                <div class="mt-1 font-mono text-[11px] uppercase tracking-[0.18em] text-muted">normalized repo-tech edges</div>
              </div>
              <div class="space-y-2 text-right">
                <div class="border border-white/10 px-3 py-1 font-mono text-[11px] text-muted-strong">[{section['share']:.1f}% of edges]</div>
                <div class="border border-white/10 px-3 py-1 font-mono text-[11px] text-muted-strong">[{fmt_int(section['repo_count'])} repos / {section['repo_share']:.1f}%]</div>
              </div>
            </div>
            <div class="mt-6 space-y-4">
              {tech_bar_rows(section['rows'], final_repos, SECTION_ACCENTS.get(section['category_id'], 'from-tropic-lagoon to-tropic-coral'))}
            </div>
          </div>
        </article>
        """
        for section in primary_category_sections
    )

    thin_category_rows = "\n".join(
        f"""
        <div class="grid grid-cols-1 gap-3 border-t border-line py-4 first:border-t-0 first:pt-0 last:pb-0 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
          <div class="min-w-0">
            <div class="text-sm font-medium text-ink">{escape(section['label'])}</div>
            <div class="mt-1 text-xs text-muted">{fmt_int(section['count'])} normalized repo-tech edges across {fmt_int(section['repo_count'])} repos. Thin categories stay listed here until coverage is broad enough for a full card.</div>
            <div class="mt-2 text-xs text-muted">{escape(', '.join(f"{row['label']} {fmt_int(row['count'])}" for row in section['rows']))}</div>
          </div>
          <div class="font-mono text-xs text-muted lg:text-right">[{section['share']:.1f}% edges • {section['repo_share']:.1f}% repos]</div>
        </div>
        """
        for section in thin_category_sections
    )

    pair_rows = "\n".join(
        f"""
        <div class="grid grid-cols-1 gap-2 border-t border-line py-3 first:border-t-0 first:pt-0 last:pb-0 sm:grid-cols-[minmax(0,1fr)_auto] sm:gap-3">
          <div class="min-w-0">
            <div class="break-words text-sm font-medium text-ink">{escape(item['left'])} <span class="text-muted">::</span> {escape(item['right'])}</div>
            <div class="mt-1 text-xs text-muted">Shared by major OSS AI repos in the normalized graph.</div>
          </div>
          <div class="font-mono text-sm text-ink sm:text-right">[{fmt_int(item['count'])}]</div>
        </div>
        """
        for item in strongest_pairs
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>OSS AI Stack Report · {escape(snapshot_date)}</title>
    <meta
      name="description"
      content="A minimal static report summarizing the 2026-03-25 OSS AI Stack Map snapshot."
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
  </head>
  <body class="min-h-screen overflow-x-hidden bg-cloud font-sans text-ink antialiased">
    <main class="mx-auto max-w-7xl px-6 py-8 sm:px-8 lg:px-10">
      <nav class="mb-8 flex flex-col gap-4 border border-line bg-paper px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">OSS AI Stack Map</div>
          <div class="mt-1 text-sm text-muted">Research publication layer for the current validated snapshot.</div>
        </div>
        <div class="flex flex-wrap gap-3">
          <a href="index.html" class="border border-ink bg-ink px-4 py-2 text-sm font-medium text-paper">Report</a>
          <a href="explainer.html" class="border border-line bg-cloud px-4 py-2 text-sm font-medium text-ink">Explainer</a>
        </div>
      </nav>
      <section class="overflow-hidden border border-line bg-ink px-6 py-8 text-paper sm:px-8 lg:px-10">
        <div class="grid gap-10 lg:grid-cols-[minmax(0,1.4fr)_22rem]">
          <div class="min-w-0">
            <div class="w-full max-w-full overflow-x-auto overscroll-x-contain">
              <pre class="inline-block whitespace-pre font-mono text-[10px] leading-5 text-muted-strong sm:text-[11px]">{escape(ascii_banner)}</pre>
            </div>
            <h1 class="mt-5 max-w-4xl text-4xl font-semibold tracking-[-0.04em] text-white sm:text-5xl lg:text-6xl">
              What major open source AI repos actually use.
            </h1>
            <p class="mt-6 max-w-3xl text-base leading-8 text-paper/80 sm:text-lg">
              This report reads directly from <span class="break-all font-mono text-paper">{escape(str(input_dir))}</span> and summarizes the current
              stack choices across the project’s final GitHub AI set.
            </p>
            <p class="mt-5 max-w-3xl text-sm leading-7 text-muted-strong">
              Study frame: GitHub-only, public, non-fork, non-archived, active within 1 month, and at least 1,000 stars. Published stack edges come from manifests, SBOMs, bounded import fallback, repo identity, and reviewed README fallback when an included repo would otherwise remain unmapped.
            </p>
            <div class="mt-6 flex flex-wrap gap-3">
              <a href="explainer.html" class="border border-white/20 px-4 py-2 text-sm font-medium text-paper">Read the explainer</a>
            </div>
          </div>

          <aside class="border border-white/10 p-6">
            <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[snapshot]</div>
            <div class="mt-4 text-3xl font-semibold tracking-tight text-paper">{escape(snapshot_date)}</div>
            <div class="mt-6 space-y-4">
              {list_rows([
                  ("Run status", str(run_state.get("status", "unknown")).title()),
                  ("Started", str(run_state.get("started_at", "unknown")).replace("T", " ").replace("+00:00", " UTC")),
                  ("Updated", str(run_state.get("updated_at", "unknown")).replace("T", " ").replace("+00:00", " UTC")),
                  ("Generated", generated_at),
              ])}
            </div>
          </aside>
        </div>
      </section>

      <section class="mt-8 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {stat_card("Discovered repos", fmt_int(total_repos), "Candidate universe collected from GitHub discovery queries and manual seeds.")}
        {stat_card("Final major AI repos", fmt_int(final_repos), f"{pct_text(final_repos, total_repos)} of discovered repos survive the full inclusion filter.")}
        {stat_card("Normalized technology edges", fmt_int(len(unique_repo_tech_pairs)), f"{fmt_int(repos_with_edges)} repos map to at least one of {fmt_int(len(technologies))} tracked technologies.")}
        {stat_card("Median final repo stars", fmt_int(median_stars), f"P75 is {fmt_int(p75_stars)} stars across the published final set.")}
      </section>

      <section class="mt-14">
        <div class="max-w-3xl">
          <div class="font-mono text-[11px] uppercase tracking-[0.24em] text-muted">+ main findings</div>
          <h2 class="mt-3 text-3xl font-semibold tracking-[-0.03em] text-ink">The modern OSS AI stack is provider-first, orchestration-heavy, and surprisingly multi-vendor.</h2>
        </div>
        <div class="mt-8 grid gap-5 lg:grid-cols-3">
          {finding_cards}
        </div>
      </section>

      <section class="mt-14 grid gap-6 lg:grid-cols-[1.15fr_0.85fr]">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ stack profile</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">A compact read on the default stack shape</h2>
          <p class="mt-4 max-w-2xl text-sm leading-7 text-muted">{escape(modal_stack_summary)}</p>
          <div class="mt-6 grid gap-4 sm:grid-cols-2">
            <div class="border border-line bg-ink p-5 text-paper">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[who ships it]</div>
              <div class="mt-4 space-y-4">
                {tech_bar_rows(owner_rows, final_repos, 'from-tropic-lagoon to-tropic-seafoam')}
              </div>
            </div>
            <div class="border border-line bg-ink p-5 text-paper">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[language mix]</div>
              <div class="mt-4 space-y-4">
                {tech_bar_rows(top_language_rows, final_repos, 'from-tropic-coral to-tropic-papaya')}
              </div>
            </div>
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ coverage and method</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Evidence tiers, validation audit, and where the map still undercounts</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Direct-supported repos", fmt_int(evidence_direct_supported), "Final repos with manifest, SBOM, import, or repo-identity evidence and no dependence on README-only fallback for coverage.")}
            {stat_card("Fallback-supported repos", fmt_int(evidence_fallback_supported), "Final repos mapped once reviewed README fallback is allowed for otherwise unmapped repos.")}
            {stat_card("README-only repos", fmt_int(evidence_readme_only), "Low-confidence fallback repos that remain explicit and separately inspectable in the publication artifact.")}
          </div>
          <div class="mt-6 space-y-4">
            <div class="border border-line bg-cloud p-5">
              <div class="text-sm font-medium text-ink">[1] Evidence tiers are now explicit</div>
              <p class="mt-2 text-sm leading-7 text-muted">{fmt_int(final_with_manifest)} final repos have manifests and {fmt_int(final_with_sbom)} have SBOM dependency evidence. {fmt_int(evidence_repo_identity_only)} repos map only via canonical repo identity, {fmt_int(evidence_mixed_fallback)} combine direct evidence with fallback signals, and {fmt_int(evidence_readme_only)} remain README-only.</p>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="text-sm font-medium text-ink">[2] Normalization still leaves gaps</div>
              <p class="mt-2 text-sm leading-7 text-muted">{fmt_int(evidence_unmapped)} included repos ({pct_text(evidence_unmapped, final_repos)}) have no normalized technology edge, so graph-like analysis describes the mapped subset, not the entire final population.</p>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="text-sm font-medium text-ink">[3] Validation is now an audit layer</div>
              <p class="mt-2 text-sm leading-7 text-muted">{escape(judge_summary)} The validation audit reviewed {fmt_int(validation_audit_sample_count)} repos, changed {fmt_int(validation_audit_changed_count)} decisions, excluded {fmt_int(validation_audit_exclusion_count)} repos from the sampled final set, and estimates a false-positive rate of {100 * validation_audit_false_positive_rate:.1f}% with a 95% interval of {100 * float(validation_audit_false_positive_ci.get('lower', 0.0)):.1f}% to {100 * float(validation_audit_false_positive_ci.get('upper', 0.0)):.1f}%.</p>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[evidence profiles]</div>
              <div class="mt-4">
                {evidence_profile_rows}
              </div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[validation sample by segment]</div>
              <div class="mt-4">
                {validation_segment_rows}
              </div>
            </div>
          </div>
        </article>
      </section>

      <section class="mt-14 grid gap-6 xl:grid-cols-[0.92fr_1.08fr]">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ research gaps</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Where normalization still misses stack evidence</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Missing-edge repos", fmt_int(gap_missing_edges_count), "Final repos that still have no normalized technology edge.")}
            {stat_card("Unmapped dep gaps", fmt_int(gap_missing_edges_unmapped), "Missing-edge finals that do have dependency evidence, but none of it normalizes yet.")}
            {stat_card("No-dependency gaps", fmt_int(gap_missing_edges_no_dep), "Missing-edge finals that have no dependency evidence at all.")}
            {stat_card("AI-specific prefixes", fmt_int(len(gap_ai_specific_prefixes)), "High-signal unresolved package families that still look AI-stack specific.")}
            {stat_card("Commodity prefixes", fmt_int(len(gap_commodity_prefixes)), "Generic tooling and ecosystem noise separated from the research backlog.")}
            {stat_card("Vendor-like repos", fmt_int(len(gap_vendor_like_unmapped)), "Repos that look vendor-related but are not mapped to a canonical identity.")}
          </div>
          <div class="mt-6 grid gap-5 lg:grid-cols-2">
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[ai-specific unmatched prefixes]</div>
              <div class="mt-4">
                {gap_prefix_rows}
              </div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[largest missing-edge repos]</div>
              <div class="mt-4">
                {gap_repo_rows}
              </div>
            </div>
            <div class="border border-line bg-cloud p-5 lg:col-span-2">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[commodity and tooling backlog]</div>
              <div class="mt-4">
                {gap_commodity_prefix_rows}
              </div>
            </div>
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ benchmark recall</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">How well the map covers the current benchmark panel, holdout set, and negative controls</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Positive entities", fmt_int(benchmark_entities), "Tracked OSS AI stack entities used for recall measurement across this snapshot.")}
            {stat_card("Negative controls", fmt_int(benchmark_negative_entities), "Discovered candidate repos that should stay out of the final set and act as a precision guardrail.")}
            {stat_card("Holdout entities", fmt_int(benchmark_holdout_entities), "Positive entities reserved as a holdout slice instead of the main tuning panel.")}
          </div>
          <div class="mt-4 grid gap-4 sm:grid-cols-3">
            {stat_card("Failed thresholds", fmt_int(len(benchmark_failed_thresholds)), "Recall metrics currently below configured minimums.")}
            {stat_card("Prioritized gaps", fmt_int(len(benchmark_prioritized_gaps)), "Benchmarks needing the next registry or discovery fixes.")}
            {stat_card("Total benchmark entries", fmt_int(benchmark_total_entities), "Combined positives and negatives now tracked by the benchmark report.")}
          </div>
          <div class="mt-6 grid gap-5 lg:grid-cols-2">
            <div class="border border-line bg-ink p-5 text-paper">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[coverage rates]</div>
              <div class="mt-4">
                {pill_rows(benchmark_rows or [("Benchmark data", "not available")])}
              </div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[top benchmark gaps]</div>
              <div class="mt-4">
                {benchmark_gap_rows}
              </div>
            </div>
          </div>
        </article>
      </section>

      <section class="mt-14">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ robustness checks</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">How much the topline changes under stricter evidence and rule-only views</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Rule-only final repos", fmt_int(int(robustness_rule_only.get("final_repo_count", 0))), "Final-set size if the project uses raw rule outputs with no judge adjustment.")}
            {stat_card("Judge-adjusted final repos", fmt_int(int(robustness_judge_adjusted.get("final_repo_count", final_repos))), "Published final-set size after hardening and validation overrides are applied.")}
            {stat_card("Judge-changed finals", fmt_int(int(robustness_judge_adjusted.get("judge_changed_final_repo_count", 0))), "Repos whose final-set status differs between rule-only and published judge-adjusted views.")}
          </div>
          <div class="mt-4 grid gap-4 sm:grid-cols-3">
            {stat_card("Direct-supported repos", fmt_int(evidence_direct_supported), "Mapped repos using direct evidence or repo identity only.")}
            {stat_card("Fallback lift", fmt_int(evidence_fallback_supported - evidence_direct_supported), "Additional mapped repos recovered only when reviewed README fallback is enabled.")}
            {stat_card("Temporal delta", fmt_int(int((robustness_temporal or {}).get("final_repo_delta", 0))), "Change in final repos relative to the baseline snapshot used for this validation pass.")}
          </div>
          <div class="mt-6 border border-line bg-cloud p-5">
            <p class="text-sm leading-7 text-muted">Rule-only yields {fmt_int(int(robustness_rule_only.get("final_repo_count", 0)))} final repos. Judge adjustment yields {fmt_int(int(robustness_judge_adjusted.get("final_repo_count", final_repos)))}. Direct-only evidence maps {fmt_int(evidence_direct_supported)} repos, and reviewed fallback lifts that to {fmt_int(evidence_fallback_supported)}. {escape('Baseline comparison: ' + str((robustness_temporal or {}).get('baseline_snapshot_dir', 'not available')) if robustness_temporal else 'No temporal baseline comparison is available for this snapshot.')}</p>
          </div>
        </article>
      </section>

      <section class="mt-14">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ technology discovery</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Post-filtered canonical candidates from the raw unmatched package graph</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Raw candidate families", fmt_int(int((technology_discovery_report or {}).get("candidate_count", 0))), "Unmatched technology families inferred from scraped dependency evidence before curation filters.")}
            {stat_card("Graph nodes", fmt_int(int((technology_discovery_report or {}).get("graph_node_count", 0))), "Package-family nodes in the projected co-usage graph.")}
            {stat_card("Filtered candidates", fmt_int(int((registry_suggestions_report or {}).get("suggestion_count", 0))), "Canonical vendor, product, or package-family candidates that survive the registry filter.")}
          </div>
          <div class="mt-6 border border-line bg-cloud p-5">
            {technology_candidate_rows}
          </div>
        </article>
      </section>

      <section class="mt-14">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ registry suggestions</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Canonical vendor, product, and package-family suggestions</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-3">
            {stat_card("Suggestions", fmt_int(int((registry_suggestions_report or {}).get("suggestion_count", 0))), "Candidates produced after filtering already-covered families and suppressing abstract capability labels.")}
            {stat_card("LLM reviewed", fmt_int(int((registry_suggestions_report or {}).get("llm_reviewed_count", 0))), "Optional OpenAI registry reviews attached to candidate suggestions.")}
            {stat_card("Shown here", fmt_int(min(len(registry_suggestions), 6)), "Top suggestion rows displayed in this report.")}
          </div>
          <div class="mt-6 border border-line bg-cloud p-5">
            {registry_suggestion_rows}
          </div>
        </article>
      </section>

      <section class="mt-14">
        <div class="max-w-3xl">
          <div class="font-mono text-[11px] uppercase tracking-[0.24em] text-muted">+ modern ai stack layers</div>
          <h2 class="mt-3 text-3xl font-semibold tracking-[-0.03em] text-ink">Where normalized stack usage concentrates</h2>
          <p class="mt-3 text-sm leading-7 text-muted">These cards rank categories by normalized repo-tech edges, not by architectural importance. Each card now shows both edge share and repo prevalence across the {fmt_int(final_repos)} final repos. Very thin categories are summarized separately below.</p>
        </div>
        <div class="mt-8 grid gap-5 lg:grid-cols-2 2xl:grid-cols-3">
          {category_cards}
        </div>
        {'<article class="mt-6 border border-line bg-paper p-6"><div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ thinly tracked layers</div><h3 class="mt-3 text-xl font-semibold tracking-tight text-ink">Visible, but not broad enough for a primary card</h3><div class="mt-6">' + thin_category_rows + '</div></article>' if thin_category_sections else ''}
      </section>

      <section class="mt-14 grid gap-6 xl:grid-cols-[0.92fr_1.08fr]">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ top technologies</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">The most repeated building blocks</h2>
          <div class="mt-6 border border-line bg-ink p-5">
            <div class="space-y-4">
              {tech_bar_rows(top_tech_rows, final_repos, 'from-tropic-lagoon to-tropic-coral')}
            </div>
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ provider and segment mix</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Who dominates, and what gets built</h2>
          <div class="mt-6 grid gap-5 lg:grid-cols-2">
            <div class="border border-line bg-ink p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[provider prevalence]</div>
              <div class="mt-4 space-y-4">
                {tech_bar_rows(top_provider_rows, final_repos, 'from-tropic-lagoon to-tropic-coral')}
              </div>
            </div>
            <div class="border border-line bg-ink p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted-strong">[primary segments]</div>
              <div class="mt-4 space-y-4">
                {tech_bar_rows(top_segment_rows, final_repos, 'from-tropic-kiwi to-tropic-papaya')}
              </div>
            </div>
          </div>
        </article>
      </section>

      <section class="mt-14 grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ repeated combinations</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">The strongest co-usage patterns</h2>
          <div class="mt-6 border border-line bg-cloud p-5">
            {pair_rows}
          </div>
        </article>

        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ at a glance</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">A few stable signals from the snapshot</h2>
          <div class="mt-6 grid gap-4 sm:grid-cols-2">
            {stat_card("Serious repos", fmt_int(serious_repos), pct_text(serious_repos, total_repos))}
            {stat_card("AI-relevant repos", fmt_int(ai_relevant_repos), pct_text(ai_relevant_repos, total_repos))}
            {stat_card("Repos with normalized techs", fmt_int(repos_with_edges), pct_text(repos_with_edges, final_repos))}
            {stat_card("Median mapped tech count", fmt_int(median_techs), "Across the technology-connected subset only.")}
          </div>
        </article>
      </section>

      <section class="mt-14">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ graph structure</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">Which technologies sit at the center of the mapped stack</h2>
          <p class="mt-4 max-w-3xl text-sm leading-7 text-muted">These visuals summarize the technology-connected subset of the final population. Eigenvector highlights the core hubs, betweenness isolates bridge technologies, repo degree shows stack breadth per mapped repo, and category mixing shows which layers of the stack actually co-occur.</p>
          <div class="mt-6 grid gap-5 xl:grid-cols-2">
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[top eigenvector technologies]</div>
              <div class="mt-3 text-sm leading-6 text-muted">High-eigenvector technologies are not just common. They sit next to other highly connected technologies and define the graph’s center of gravity.</div>
              <div class="mt-4 overflow-x-auto">{eigenvector_chart}</div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[betweenness vs prevalence]</div>
              <div class="mt-3 text-sm leading-6 text-muted">Points higher on the chart bridge otherwise different tool combinations. Point size reflects weighted co-occurrence strength.</div>
              <div class="mt-4 overflow-x-auto">{centrality_scatter}</div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[repo degree distribution]</div>
              <div class="mt-3 text-sm leading-6 text-muted">This is the distribution of tracked technologies per mapped repo, not the full final set including no-edge repos.</div>
              <div class="mt-4 overflow-x-auto">{repo_degree_chart}</div>
            </div>
            <div class="border border-line bg-cloud p-5">
              <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">[category mixing heatmap]</div>
              <div class="mt-3 text-sm leading-6 text-muted">Weighted co-occurrence between the biggest stack categories. Darker cells indicate heavier cross-category coupling in the technology projection.</div>
              <div class="mt-4 overflow-x-auto">{category_heatmap}</div>
            </div>
          </div>
        </article>
      </section>

      <section class="mt-14">
        <article class="border border-line bg-paper p-6">
          <div class="font-mono text-[11px] uppercase tracking-[0.22em] text-muted">+ community structure</div>
          <h2 class="mt-3 text-2xl font-semibold tracking-tight text-ink">How the technology graph breaks into stack families</h2>
          <div class="mt-4 max-w-3xl text-sm leading-7 text-muted">
            Greedy modularity on the technology projection found <span class="font-mono text-ink">{fmt_int(len(communities))}</span> communities with modularity <span class="font-mono text-ink">{modularity:.4f}</span>. Lower modularity means the graph is still heavily cross-linked, so these are useful stack families rather than cleanly isolated islands.
          </div>
          <div class="mt-6 grid gap-5 xl:grid-cols-3">
            {"".join(community_cards) or stat_card("Communities", "0", "No technology communities were available in this snapshot.")}
          </div>
        </article>
      </section>

      <footer class="mt-14 border border-line bg-paper px-6 py-5 text-sm leading-7 text-muted">
        <span class="font-mono text-ink">[source]</span> staged Parquet outputs in <span class="break-all font-mono text-ink">{escape(str(input_dir))}</span>. This file is a static publication layer on top of the existing research pipeline, not a separate data source.
      </footer>
    </main>
    <script>
      (() => {{
        const buildId = {json.dumps(build_id)};
        const manifestPath = "report-latest.json";
        const stablePaths = new Set(["/", "/index.html", "/oss-ai-stack-report-latest.html"]);
        const pollMs = 15000;

        async function poll() {{
          try {{
            const response = await fetch(`${{manifestPath}}?ts=${{Date.now()}}`, {{ cache: "no-store" }});
            if (!response.ok) return;
            const manifest = await response.json();
            if (!manifest?.build_id || manifest.build_id === buildId) return;

            const path = window.location.pathname || "/";
            if (stablePaths.has(path)) {{
              window.location.reload();
              return;
            }}
            window.location.replace(manifest.stable_report || "oss-ai-stack-report-latest.html");
          }} catch (_error) {{
          }}
        }}

        window.setInterval(poll, pollMs);
      }})();
    </script>
  </body>
</html>
"""
    return html


def write_latest_entrypoints(*, output: Path, input_dir: Path) -> None:
    stable_report = output.parent / "oss-ai-stack-report-latest.html"
    entrypoint = output.parent / "index.html"
    copyfile(output, stable_report)
    copyfile(output, entrypoint)

    manifest = {
        "build_id": input_dir.name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_report": output.name,
        "stable_report": stable_report.name,
        "entrypoint": entrypoint.name,
        "input_dir": str(input_dir),
    }
    (output.parent / "report-latest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    html = build_report(args.input_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    write_latest_entrypoints(output=args.output, input_dir=args.input_dir)
    print(f"Wrote HTML report to {args.output}")


if __name__ == "__main__":
    main()
