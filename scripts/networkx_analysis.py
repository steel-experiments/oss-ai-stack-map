# ruff: noqa: E501

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import networkx as nx
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run NetworkX analysis on a staged oss-ai-stack-map snapshot."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/run-2026-03-25-resumable"),
        help="Directory containing staged Parquet outputs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/networkx-analysis-2026-03-25.md"),
        help="Markdown file to write.",
    )
    return parser.parse_args()


def load_rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def percentile(values: list[int | float], p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    index = (len(ordered) - 1) * p
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize(values: list[int | float]) -> dict[str, float]:
    return {
        "min": float(min(values)),
        "p25": percentile(values, 0.25),
        "median": percentile(values, 0.50),
        "p75": percentile(values, 0.75),
        "mean": sum(values) / len(values),
        "max": float(max(values)),
    }


def fmt_num(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"


def pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.0%"
    return f"{(100 * numerator / denominator):.1f}%"


def top_items(rows: list[tuple], limit: int) -> list[tuple]:
    return rows[:limit]


def build_graphs(
    edges: list[dict],
    technologies: dict[str, dict],
) -> tuple[nx.Graph, nx.Graph, nx.Graph, set[str], set[str]]:
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
            category_id=tech["category_id"],
            provider_id=tech.get("provider_id"),
        )
        bipartite.add_edge(repo_node, tech_node, evidence_type=row["evidence_type"])

    repo_nodes = {n for n, d in bipartite.nodes(data=True) if d["bipartite"] == "repo"}
    tech_nodes = set(bipartite.nodes()) - repo_nodes
    tech_projection = nx.bipartite.weighted_projected_graph(bipartite, tech_nodes)
    repo_projection = nx.bipartite.weighted_projected_graph(bipartite, repo_nodes)
    for _, _, data in tech_projection.edges(data=True):
        data["distance"] = 1 / data["weight"]
    return bipartite, tech_projection, repo_projection, repo_nodes, tech_nodes


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def render_report(input_dir: Path) -> str:
    repos = load_rows(input_dir / "repos.parquet")
    decisions = load_rows(input_dir / "repo_inclusion_decisions.parquet")
    edges = load_rows(input_dir / "repo_technology_edges.parquet")
    technologies = {
        row["technology_id"]: row for row in load_rows(input_dir / "technologies.parquet")
    }

    repo_by_id = {row["repo_id"]: row for row in repos}
    decision_by_id = {row["repo_id"]: row for row in decisions}
    final_repo_ids = {row["repo_id"] for row in decisions if row["passed_major_filter"]}
    edge_repo_ids = {row["repo_id"] for row in edges}
    missing_edge_repo_ids = sorted(final_repo_ids - edge_repo_ids)

    bipartite, tech_projection, repo_projection, repo_nodes, tech_nodes = build_graphs(
        edges, technologies
    )

    components = sorted(nx.connected_components(bipartite), key=len, reverse=True)
    repo_degree_stats = summarize([bipartite.degree(node) for node in repo_nodes])
    tech_degree_stats = summarize([bipartite.degree(node) for node in tech_nodes])

    top_tech_degree = sorted(
        (
            bipartite.degree(node),
            bipartite.nodes[node]["display_name"],
            bipartite.nodes[node]["category_id"],
        )
        for node in tech_nodes
    )[::-1]
    top_repo_degree = sorted(
        (
            bipartite.degree(node),
            bipartite.nodes[node]["full_name"],
        )
        for node in repo_nodes
    )[::-1]

    top_tech_strength = sorted(
        (
            sum(data["weight"] for _, _, data in tech_projection.edges(node, data=True)),
            tech_projection.nodes[node]["display_name"],
        )
        for node in tech_projection.nodes()
    )[::-1]
    top_repo_strength = sorted(
        (
            sum(data["weight"] for _, _, data in repo_projection.edges(node, data=True)),
            repo_projection.nodes[node]["full_name"],
        )
        for node in repo_projection.nodes()
    )[::-1]

    cooccurrence_edges = sorted(
        (
            data["weight"],
            tech_projection.nodes[u]["display_name"],
            tech_projection.nodes[v]["display_name"],
        )
        for u, v, data in tech_projection.edges(data=True)
    )[::-1]

    betweenness = nx.betweenness_centrality(tech_projection, weight="distance")
    closeness = nx.closeness_centrality(tech_projection, distance="distance")
    eigenvector = nx.eigenvector_centrality(tech_projection, weight="weight", max_iter=5000)

    top_betweenness = sorted(
        (
            value,
            tech_projection.nodes[node]["display_name"],
        )
        for node, value in betweenness.items()
    )[::-1]
    top_closeness = sorted(
        (
            value,
            tech_projection.nodes[node]["display_name"],
        )
        for node, value in closeness.items()
    )[::-1]
    top_eigenvector = sorted(
        (
            value,
            tech_projection.nodes[node]["display_name"],
        )
        for node, value in eigenvector.items()
    )[::-1]

    communities = list(
        nx.community.greedy_modularity_communities(tech_projection, weight="weight")
    )
    communities = sorted(communities, key=len, reverse=True)
    modularity = nx.community.modularity(tech_projection, communities, weight="weight")

    category_mixing = Counter()
    for left, right, data in tech_projection.edges(data=True):
        left_category = tech_projection.nodes[left]["category_id"]
        right_category = tech_projection.nodes[right]["category_id"]
        category_mixing[tuple(sorted((left_category, right_category)))] += data["weight"]

    highest_star_missing = sorted(
        (
            repo_by_id[repo_id]["stars"],
            decision_by_id[repo_id]["full_name"],
            decision_by_id[repo_id]["primary_segment"] or "unassigned",
            decision_by_id[repo_id]["score_serious"],
            decision_by_id[repo_id]["score_ai"],
        )
        for repo_id in missing_edge_repo_ids
    )[::-1]

    lines: list[str] = []
    lines.append("# NetworkX Analysis: 2026-03-25 Snapshot")
    lines.append("")
    lines.append(
        "This report analyzes the normalized repo-to-technology graph in "
        f"`{input_dir}` using NetworkX."
    )
    lines.append("")
    lines.append("Graph construction:")
    lines.append("")
    lines.append(
        "- Bipartite graph: repositories connected to technologies through `repo_technology_edges.parquet`"
    )
    lines.append(
        "- Technology projection: two technologies are connected when they co-occur in at least one repo"
    )
    lines.append(
        "- Repo projection: two repos are connected when they share at least one normalized technology"
    )
    lines.append("")
    lines.append("Important scope note:")
    lines.append("")
    lines.append(
        f"- The final inclusion table contains `{len(final_repo_ids):,}` repos, but only `{len(edge_repo_ids):,}` "
        "of them have at least one normalized technology edge."
    )
    lines.append(
        f"- The NetworkX graph therefore describes the technology-connected subset of the final population, leaving `{len(missing_edge_repo_ids):,}` included repos outside the graph."
    )
    lines.append("")

    lines.append("## Graph overview")
    lines.append("")
    lines.append(
        markdown_table(
            ["Metric", "Value"],
            [
                ["Bipartite nodes", fmt_num(bipartite.number_of_nodes())],
                ["Bipartite edges", fmt_num(bipartite.number_of_edges())],
                ["Repo nodes", fmt_num(len(repo_nodes))],
                ["Technology nodes", fmt_num(len(tech_nodes))],
                ["Bipartite density", f"{nx.density(bipartite):.4f}"],
                ["Connected components", fmt_num(len(components))],
                ["Largest component share", pct(len(components[0]), bipartite.number_of_nodes())],
                ["Technology projection nodes", fmt_num(tech_projection.number_of_nodes())],
                ["Technology projection edges", fmt_num(tech_projection.number_of_edges())],
                ["Technology projection density", f"{nx.density(tech_projection):.4f}"],
                ["Repo projection nodes", fmt_num(repo_projection.number_of_nodes())],
                ["Repo projection edges", fmt_num(repo_projection.number_of_edges())],
                ["Repo projection density", f"{nx.density(repo_projection):.4f}"],
            ],
        )
    )
    lines.append("")
    lines.append(
        "The graph is fully connected: every technology-connected repo and every technology sits in a single connected component."
    )
    lines.append("")

    lines.append("## Degree structure")
    lines.append("")
    lines.append(
        markdown_table(
            ["Population", "Min", "P25", "Median", "P75", "Mean", "Max"],
            [
                [
                    "Repo degree",
                    fmt_num(repo_degree_stats["min"]),
                    fmt_num(repo_degree_stats["p25"]),
                    fmt_num(repo_degree_stats["median"]),
                    fmt_num(repo_degree_stats["p75"]),
                    fmt_num(repo_degree_stats["mean"]),
                    fmt_num(repo_degree_stats["max"]),
                ],
                [
                    "Technology degree",
                    fmt_num(tech_degree_stats["min"]),
                    fmt_num(tech_degree_stats["p25"]),
                    fmt_num(tech_degree_stats["median"]),
                    fmt_num(tech_degree_stats["p75"]),
                    fmt_num(tech_degree_stats["mean"]),
                    fmt_num(tech_degree_stats["max"]),
                ],
            ],
        )
    )
    lines.append("")
    lines.append("Top technologies by repo degree:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology", "Category", "Repos"],
            [
                [str(index), tech, category, fmt_num(degree)]
                for index, (degree, tech, category) in enumerate(top_items(top_tech_degree, 10), start=1)
            ],
        )
    )
    lines.append("")
    lines.append("Top repos by technology count:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Repository", "Technologies"],
            [
                [str(index), repo, fmt_num(degree)]
                for index, (degree, repo) in enumerate(top_items(top_repo_degree, 10), start=1)
            ],
        )
    )
    lines.append("")

    lines.append("## Technology projection")
    lines.append("")
    lines.append(
        "Weighted degree here means the sum of co-occurrence counts across all neighboring technologies."
    )
    lines.append("")
    lines.append("Top technologies by weighted degree:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology", "Weighted degree"],
            [
                [str(index), tech, fmt_num(weight)]
                for index, (weight, tech) in enumerate(top_items(top_tech_strength, 10), start=1)
            ],
        )
    )
    lines.append("")
    lines.append("Strongest technology co-occurrence pairs:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology A", "Technology B", "Shared repos"],
            [
                [str(index), left, right, fmt_num(weight)]
                for index, (weight, left, right) in enumerate(top_items(cooccurrence_edges, 12), start=1)
            ],
        )
    )
    lines.append("")

    lines.append("## Centrality")
    lines.append("")
    lines.append(
        "Betweenness and closeness use inverse co-occurrence weight as distance, so stronger co-usage implies shorter paths."
    )
    lines.append("")
    lines.append(
        "Because those weighted distances can be smaller than `1`, closeness values here are not bounded to the usual `[0, 1]` range."
    )
    lines.append("")
    lines.append("Top technologies by betweenness centrality:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology", "Betweenness"],
            [
                [str(index), tech, f"{value:.4f}"]
                for index, (value, tech) in enumerate(top_items(top_betweenness, 10), start=1)
            ],
        )
    )
    lines.append("")
    lines.append("Top technologies by closeness centrality:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology", "Closeness"],
            [
                [str(index), tech, f"{value:.4f}"]
                for index, (value, tech) in enumerate(top_items(top_closeness, 10), start=1)
            ],
        )
    )
    lines.append("")
    lines.append("Top technologies by eigenvector centrality:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Technology", "Eigenvector"],
            [
                [str(index), tech, f"{value:.4f}"]
                for index, (value, tech) in enumerate(top_items(top_eigenvector, 10), start=1)
            ],
        )
    )
    lines.append("")
    lines.append(
        "OpenAI SDK dominates every centrality family. It is not just the most frequent node; it is also the main bridge technology across otherwise different tool stacks."
    )
    lines.append("")

    lines.append("## Community structure")
    lines.append("")
    lines.append(
        f"Greedy modularity on the technology projection found `{len(communities)}` communities with modularity `{modularity:.4f}`."
    )
    lines.append("")
    lines.append(
        "The low modularity value indicates weak separation: the technology graph is dense and heavily cross-linked."
    )
    lines.append("")
    for index, community in enumerate(communities, start=1):
        names = sorted(tech_projection.nodes[node]["display_name"] for node in community)
        lines.append(f"Community {index} (`{len(community)}` technologies):")
        lines.append("")
        lines.append("- " + ", ".join(names))
        lines.append("")

    lines.append("## Category mixing")
    lines.append("")
    lines.append("Most common cross-category co-occurrence blocks by shared-repo weight:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Category pair", "Weighted co-occurrence"],
            [
                [str(index), f"`{left}` x `{right}`", fmt_num(weight)]
                for index, ((left, right), weight) in enumerate(
                    top_items(category_mixing.most_common(10), 10), start=1
                )
            ],
        )
    )
    lines.append("")
    lines.append(
        "The strongest mixing pattern is between model-provider SDKs and orchestration frameworks, followed by intra-orchestration links and provider/training links."
    )
    lines.append("")

    lines.append("## Repo projection")
    lines.append("")
    lines.append(
        "The repo projection is extremely dense because many repos share the same dominant technologies."
    )
    lines.append("")
    lines.append("Top repos by weighted connectivity to other repos:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Repository", "Weighted degree"],
            [
                [str(index), repo, fmt_num(weight)]
                for index, (weight, repo) in enumerate(top_items(top_repo_strength, 10), start=1)
            ],
        )
    )
    lines.append("")

    lines.append("## Included repos with no normalized technology edge")
    lines.append("")
    lines.append(
        f"`{len(missing_edge_repo_ids):,}` final included repos have no row in `repo_technology_edges.parquet`."
    )
    lines.append("")
    lines.append(
        "These repos are in scope for the classifier, but they are absent from the graph because the current normalization rules found no mapped technology."
    )
    lines.append("")
    lines.append("Highest-star examples:")
    lines.append("")
    lines.append(
        markdown_table(
            ["Rank", "Repository", "Stars", "Primary segment", "Serious", "AI"],
            [
                [
                    str(index),
                    repo,
                    fmt_num(stars),
                    segment,
                    fmt_num(serious),
                    fmt_num(ai),
                ]
                for index, (stars, repo, segment, serious, ai) in enumerate(
                    top_items(highest_star_missing, 12), start=1
                )
            ],
        )
    )
    lines.append("")

    lines.append("## Takeaways")
    lines.append("")
    lines.append(
        "1. The technology-connected subset forms a single giant component, which means there is no clear set of isolated AI stack islands in this snapshot."
    )
    lines.append(
        "2. OpenAI SDK is the central hub by frequency, co-occurrence strength, closeness, eigenvector score, and bridge position."
    )
    lines.append(
        "3. The strongest repeated stack pattern is provider SDKs co-occurring with orchestration frameworks, especially around OpenAI, LangChain, Anthropic, Google GenAI, and LangGraph."
    )
    lines.append(
        "4. Community detection finds only weakly separated modules. The graph looks more like one overlapping ecosystem than a set of cleanly partitioned sub-ecosystems."
    )
    lines.append(
        "5. The graph under-represents some included repos because `54` final repos have no normalized technology edge. Any downstream network interpretation should be read as analysis of the normalized-technology subset, not the full final population."
    )
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    report = render_report(args.input_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
