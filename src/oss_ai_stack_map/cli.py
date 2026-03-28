from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from oss_ai_stack_map.analysis.snapshot import (
    append_experiment_ledger_entry,
    build_snapshot_manifest,
    compare_snapshots,
    refresh_snapshot_contexts,
    render_snapshot_comparison_markdown,
    render_snapshot_validation_markdown,
    repair_snapshot,
    validate_snapshot,
    write_snapshot_docs,
)
from oss_ai_stack_map.config.loader import ConfigValidationError, load_runtime
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import StageTiming
from oss_ai_stack_map.pipeline.classification import classify_candidates
from oss_ai_stack_map.pipeline.discovery import (
    discover_candidates,
    select_preflight_repositories,
)
from oss_ai_stack_map.pipeline.registry_suggestions import build_registry_suggestion_report
from oss_ai_stack_map.pipeline.reporting import build_report_summary
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()
ConfigDirOption = Annotated[
    Path,
    typer.Option("--config-dir", exists=True, file_okay=False, dir_okay=True),
]
OutputDirOption = Annotated[Path, typer.Option("--output-dir")]
InputDirOption = Annotated[
    Path,
    typer.Option("--input-dir", exists=True, file_okay=False, dir_okay=True),
]
DocsDirOption = Annotated[
    Path | None,
    typer.Option("--docs-dir", file_okay=False, dir_okay=True),
]
DocsDirRequiredOption = Annotated[
    Path,
    typer.Option("--docs-dir", file_okay=False, dir_okay=True),
]
LedgerPathOption = Annotated[
    Path,
    typer.Option("--ledger-path", file_okay=True, dir_okay=False),
]
WriteManifestOption = Annotated[bool, typer.Option("--write-manifest/--no-write-manifest")]
LeftDirOption = Annotated[
    Path,
    typer.Option("--left-dir", exists=True, file_okay=False, dir_okay=True),
]
RightDirOption = Annotated[
    Path,
    typer.Option("--right-dir", exists=True, file_okay=False, dir_okay=True),
]
OptionalIntOption = Annotated[int | None, typer.Option(min=1)]
JudgeOption = Annotated[bool | None, typer.Option("--judge/--no-judge")]
JudgeHardeningOption = Annotated[
    bool | None,
    typer.Option("--judge-hardening/--no-judge-hardening"),
]
JudgeValidationOption = Annotated[
    bool | None,
    typer.Option("--judge-validation/--no-judge-validation"),
]
FilesChangedOption = Annotated[list[str], typer.Option("--files-changed")]


def log_progress(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S", time.gmtime())
    console.print(f"[dim]{timestamp}[/dim] {message}")


def require_github_token(runtime) -> None:
    if runtime.env.github_token:
        return
    console.print("Missing GITHUB_TOKEN for GitHub API command.")
    raise typer.Exit(code=1)


def write_stage_timings(
    output_dir: Path,
    timings: list[StageTiming],
    write_csv: bool,
) -> None:
    checkpoint_path = output_dir / "checkpoints" / "stage_timings.parquet"
    checkpoint_timings: list[StageTiming] = []
    if checkpoint_path.exists():
        checkpoint_timings = read_parquet_models(checkpoint_path, StageTiming)
    if not timings and not checkpoint_timings:
        return
    existing_path = output_dir / "stage_timings.parquet"
    existing: list[StageTiming] = []
    if existing_path.exists():
        existing = read_parquet_models(existing_path, StageTiming)
    merged = dedupe_stage_timings([*existing, *checkpoint_timings, *timings])
    write_rows(
        output_dir,
        "stage_timings",
        [timing.to_row() for timing in merged],
        write_csv=write_csv,
    )


def dedupe_stage_timings(timings: list[StageTiming]) -> list[StageTiming]:
    seen: set[tuple[str, float, int | None, str | None, str | None]] = set()
    deduped: list[StageTiming] = []
    for timing in timings:
        key = (
            timing.stage_id,
            round(timing.seconds, 6),
            timing.item_count,
            timing.notes,
            timing.attempt_id,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(timing)
    return deduped


def apply_judge_mode_overrides(
    *,
    runtime,
    judge: bool | None,
    judge_hardening: bool | None,
    judge_validation: bool | None,
) -> None:
    if judge is not None:
        runtime.study.judge.enabled = judge
        runtime.study.judge.hardening_enabled = judge
        runtime.study.judge.validation_enabled = judge
    if judge_hardening is not None:
        runtime.study.judge.hardening_enabled = judge_hardening
    if judge_validation is not None:
        runtime.study.judge.validation_enabled = judge_validation


@app.command()
def rate_limit(config_dir: ConfigDirOption = Path("config")) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    with GitHubClient(runtime=runtime) as client:
        data = client.get_rate_limit()

    resources = data.get("resources", {})
    table = Table(title="GitHub Rate Limits")
    table.add_column("Bucket")
    table.add_column("Limit", justify="right")
    table.add_column("Remaining", justify="right")
    table.add_column("Reset", justify="right")

    for name, payload in sorted(resources.items()):
        table.add_row(
            name,
            str(payload.get("limit", "")),
            str(payload.get("remaining", "")),
            str(payload.get("reset", "")),
        )

    console.print(table)


@app.command("validate-config")
def validate_config(config_dir: ConfigDirOption = Path("config")) -> None:
    try:
        runtime = load_runtime(config_dir=config_dir)
    except ConfigValidationError as exc:
        console.print(str(exc))
        raise typer.Exit(code=1) from exc
    console.print(f"Config OK: {runtime.config_dir}")


@app.command()
def discover(
    config_dir: ConfigDirOption = Path("config"),
    output_dir: OutputDirOption = Path("data/staged"),
    max_pages_per_query: OptionalIntOption = None,
    max_repos: OptionalIntOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    if max_pages_per_query is not None:
        runtime.study.filters.max_search_pages_per_query = max_pages_per_query
    if max_repos is not None:
        runtime.study.filters.max_repos = max_repos

    with GitHubClient(runtime=runtime) as client:
        result = discover_candidates(
            runtime=runtime,
            client=client,
            output_dir=output_dir,
            progress=log_progress,
        )

    write_stage_timings(
        output_dir=output_dir,
        timings=result.stage_timings,
        write_csv=runtime.study.outputs.write_csv,
    )

    console.print(
        "Discovered "
        f"{len(result.repositories)} unique repositories from "
        f"{len(result.queries)} queries."
    )
    console.print(f"Wrote candidates to {output_dir}")


@app.command("discovery-preflight")
def discovery_preflight(
    config_dir: ConfigDirOption = Path("config"),
    output_dir: OutputDirOption = Path("data/preflight"),
    max_pages_per_query: Annotated[int, typer.Option(min=1)] = 1,
    max_repos: Annotated[int, typer.Option(min=1)] = 250,
    sample_size: Annotated[int, typer.Option(min=1)] = 12,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    runtime.study.filters.max_search_pages_per_query = max_pages_per_query
    runtime.study.filters.max_repos = max_repos

    with GitHubClient(runtime=runtime) as client:
        result = discover_candidates(
            runtime=runtime,
            client=client,
            output_dir=output_dir,
            progress=log_progress,
        )
        sampled_repos = select_preflight_repositories(
            runtime=runtime,
            repositories=result.repositories,
            sample_size=sample_size,
        )
        sample_checks: list[tuple[str, str, int]] = []
        for repo in sampled_repos:
            owner, name = repo.full_name.split("/", 1)
            tree_paths = client.get_tree(owner, name, repo.default_branch)
            sample_checks.append((repo.full_name, repo.default_branch, len(tree_paths)))

    head_count = sum(1 for repo in result.repositories if repo.default_branch == "HEAD")
    empty_tree_count = sum(1 for _, _, tree_count in sample_checks if tree_count == 0)

    summary = Table(title="Discovery Preflight")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Discovered repos", str(len(result.repositories)))
    summary.add_row("Queries", str(len(result.queries)))
    summary.add_row("Repos with default_branch=HEAD", str(head_count))
    summary.add_row("Sampled repos", str(len(sample_checks)))
    summary.add_row("Sampled repos with empty trees", str(empty_tree_count))
    console.print(summary)

    details = Table(title="Sampled Repo Checks")
    details.add_column("Repo")
    details.add_column("Branch")
    details.add_column("Tree paths", justify="right")
    for full_name, default_branch, tree_count in sample_checks:
        details.add_row(full_name, default_branch, str(tree_count))
    console.print(details)

    failures: list[str] = []
    if head_count:
        failures.append(f"{head_count} discovered repos still have default_branch=HEAD")
    if empty_tree_count:
        failures.append(f"{empty_tree_count} sampled repos returned an empty tree")

    if failures:
        for failure in failures:
            console.print(failure)
        raise typer.Exit(code=1)

    console.print(f"Preflight OK: wrote discovery sample to {output_dir}")


@app.command()
def classify(
    config_dir: ConfigDirOption = Path("config"),
    input_dir: InputDirOption = Path("data/staged"),
    output_dir: OutputDirOption = Path("data/staged"),
    limit: OptionalIntOption = None,
    judge: JudgeOption = None,
    judge_hardening: JudgeHardeningOption = None,
    judge_validation: JudgeValidationOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    apply_judge_mode_overrides(
        runtime=runtime,
        judge=judge,
        judge_hardening=judge_hardening,
        judge_validation=judge_validation,
    )
    with GitHubClient(runtime=runtime) as client:
        summary = classify_candidates(
            runtime=runtime,
            client=client,
            input_dir=input_dir,
            output_dir=output_dir,
            limit=limit,
            progress=log_progress,
        )

    write_stage_timings(
        output_dir=output_dir,
        timings=summary.stage_timings,
        write_csv=runtime.study.outputs.write_csv,
    )

    console.print(
        "Classification complete: "
        f"{summary.passed_serious}/{summary.total} serious, "
        f"{summary.passed_ai}/{summary.total} AI-relevant, "
        f"{summary.passed_major}/{summary.total} final."
    )


@app.command()
def snapshot(
    config_dir: ConfigDirOption = Path("config"),
    output_dir: OutputDirOption = Path("data/staged"),
    max_pages_per_query: OptionalIntOption = None,
    max_repos: OptionalIntOption = None,
    judge: JudgeOption = None,
    judge_hardening: JudgeHardeningOption = None,
    judge_validation: JudgeValidationOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    if max_pages_per_query is not None:
        runtime.study.filters.max_search_pages_per_query = max_pages_per_query
    if max_repos is not None:
        runtime.study.filters.max_repos = max_repos
    apply_judge_mode_overrides(
        runtime=runtime,
        judge=judge,
        judge_hardening=judge_hardening,
        judge_validation=judge_validation,
    )

    with GitHubClient(runtime=runtime) as client:
        snapshot_started_at = time.perf_counter()
        discovered = discover_candidates(
            runtime=runtime,
            client=client,
            output_dir=output_dir,
            progress=log_progress,
        )
        summary = classify_candidates(
            runtime=runtime,
            client=client,
            input_dir=output_dir,
            output_dir=output_dir,
            limit=runtime.study.filters.max_repos,
            progress=log_progress,
        )

    snapshot_timings = [
        *discovered.stage_timings,
        *summary.stage_timings,
        StageTiming(
            stage_id="snapshot_total",
            seconds=time.perf_counter() - snapshot_started_at,
            item_count=summary.total,
        ),
    ]
    write_stage_timings(
        output_dir=output_dir,
        timings=snapshot_timings,
        write_csv=runtime.study.outputs.write_csv,
    )

    console.print(
        "Snapshot complete: "
        f"{len(discovered.repositories)} discovered, "
        f"{summary.passed_major} included in final major set."
    )


@app.command()
def report(
    config_dir: ConfigDirOption = Path("config"),
    input_dir: InputDirOption = Path("data/staged"),
    top_n: OptionalIntOption = 10,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    summary = build_report_summary(input_dir=input_dir, top_n=top_n or 10, runtime=runtime)

    universe = Table(title="Universe Summary")
    universe.add_column("Metric")
    universe.add_column("Value", justify="right")
    universe.add_row("Discovered repos", str(summary.total_repos))
    universe.add_row("Serious repos", str(summary.serious_repos))
    universe.add_row("AI-relevant repos", str(summary.ai_relevant_repos))
    universe.add_row("Final repos", str(summary.final_repos))
    console.print(universe)

    tech_table = Table(title="Top Technologies")
    tech_table.add_column("Technology")
    tech_table.add_column("Category")
    tech_table.add_column("Repos", justify="right")
    tech_table.add_column("Share", justify="right")
    for row in summary.top_technologies:
        tech_table.add_row(
            row["display_name"],
            row.get("category_id") or "",
            str(row["repo_count"]),
            f"{row['repo_share']:.0%}",
        )
    console.print(tech_table)

    provider_table = Table(title="Top Providers")
    provider_table.add_column("Provider")
    provider_table.add_column("Repos", justify="right")
    provider_table.add_column("Share", justify="right")
    for row in summary.top_providers:
        provider_table.add_row(
            row["provider_id"],
            str(row["repo_count"]),
            f"{row['repo_share']:.0%}",
        )
    console.print(provider_table)

    gaps = Table(title="Coverage Gaps")
    gaps.add_column("Metric")
    gaps.add_column("Value", justify="right")
    gaps.add_row(
        "Final repos missing edges",
        str(summary.gap_report.final_repos_missing_edges_count),
    )
    gaps.add_row(
        "Missing edges with unmapped dependency evidence",
        str(summary.gap_report.final_repos_missing_edges_with_unmapped_dependency_evidence_count),
    )
    gaps.add_row(
        "Missing edges with no dependency evidence",
        str(summary.gap_report.final_repos_missing_edges_with_no_dependency_evidence_count),
    )
    gaps.add_row(
        "Top unmatched packages tracked",
        str(len(summary.gap_report.top_unmatched_packages)),
    )
    gaps.add_row(
        "AI-specific unmatched prefixes",
        str(len(summary.gap_report.top_ai_specific_unmatched_package_prefixes)),
    )
    gaps.add_row(
        "Commodity/tooling prefixes",
        str(len(summary.gap_report.top_commodity_unmatched_package_prefixes)),
    )
    gaps.add_row(
        "Vendor-like unmapped repos tracked",
        str(len(summary.gap_report.top_vendor_like_unmapped_repos)),
    )
    console.print(gaps)

    if summary.benchmark_recall_report is not None:
        benchmark = Table(title="Benchmark Recall")
        benchmark.add_column("Metric")
        benchmark.add_column("Value", justify="right")
        benchmark.add_row("Benchmarked entities", str(summary.benchmark_recall_report.entity_count))
        benchmark.add_row(
            "Repo discovered",
            f"{summary.benchmark_recall_report.entities_with_repo_discovered}/{summary.benchmark_recall_report.entity_count}",
        )
        benchmark.add_row(
            "Repo discovered by broad search",
            (
                f"{summary.benchmark_recall_report.entities_with_repo_discovered_by_search}/"
                f"{summary.benchmark_recall_report.entity_count}"
            ),
        )
        benchmark.add_row(
            "Repo discovered only via exact seed",
            (
                f"{summary.benchmark_recall_report.entities_with_repo_discovered_by_seed_only}/"
                f"{summary.benchmark_recall_report.entity_count}"
            ),
        )
        benchmark.add_row(
            "Repo included",
            f"{summary.benchmark_recall_report.entities_with_repo_included}/{summary.benchmark_recall_report.entity_count}",
        )
        benchmark.add_row(
            "Repo identity mapped",
            f"{summary.benchmark_recall_report.entities_with_repo_identity_mapped}/{summary.benchmark_recall_report.entity_count}",
        )
        benchmark.add_row(
            "Third-party adoption found",
            f"{summary.benchmark_recall_report.entities_with_third_party_adoption}/{summary.benchmark_recall_report.entity_count}",
        )
        benchmark.add_row(
            "Failed thresholds",
            str(len(summary.benchmark_recall_report.failed_thresholds)),
        )
        console.print(benchmark)

    registry_suggestions_path = input_dir / "registry_suggestions.json"
    if registry_suggestions_path.exists():
        registry_suggestions_payload = json.loads(
            registry_suggestions_path.read_text(encoding="utf-8")
        )
    else:
        registry_suggestions_payload = build_registry_suggestion_report(
            input_dir=input_dir,
            runtime=runtime,
            top_n=top_n or 10,
        ).__dict__
    discovery = Table(title="Technology Discovery")
    discovery.add_column("Family")
    discovery.add_column("Score", justify="right")
    discovery.add_column("Final", justify="right")
    discovery.add_column("Missing", justify="right")
    discovery.add_column("Example packages")
    displayed_candidates = registry_suggestions_payload.get("suggestions", [])
    for row in displayed_candidates[: min(len(displayed_candidates), 8)]:
        discovery.add_row(
            row["suggested_display_name"],
            f"{row['priority_score']:.1f}",
            str(row["final_repo_count"]),
            str(row["missing_edge_repo_count"]),
            ", ".join(row.get("suggested_package_prefixes", [])[:2]),
        )
    console.print(discovery)

    suggestions = Table(title="Registry Suggestions")
    suggestions.add_column("Candidate")
    suggestions.add_column("Category")
    suggestions.add_column("Confidence")
    suggestions.add_column("Score", justify="right")
    for row in registry_suggestions_payload.get("suggestions", [])[: min(8, top_n or 10)]:
        suggestions.add_row(
            row["suggested_display_name"],
            row["suggested_category_id"],
            row["confidence"],
            f"{row['priority_score']:.1f}",
        )
    if registry_suggestions_payload.get("suggestions"):
        console.print(suggestions)


@app.command("registry-suggestions")
def registry_suggestions(
    config_dir: ConfigDirOption = Path("config"),
    input_dir: InputDirOption = Path("data/staged"),
    top_n: OptionalIntOption = 15,
    judge: JudgeOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    payload = build_registry_suggestion_report(
        input_dir=input_dir,
        runtime=runtime,
        top_n=top_n or 15,
        judge_with_llm=bool(judge),
    )
    table = Table(title="Registry Suggestions")
    table.add_column("Candidate")
    table.add_column("Category")
    table.add_column("Confidence")
    table.add_column("Score", justify="right")
    table.add_column("Prefixes")
    for row in payload.suggestions[: min(12, top_n or 15)]:
        table.add_row(
            row["suggested_display_name"],
            row["suggested_category_id"],
            row["confidence"],
            f"{row['priority_score']:.1f}",
            ", ".join(row["suggested_package_prefixes"][:2]),
        )
    console.print(table)


@app.command("snapshot-validate")
def snapshot_validate(
    input_dir: InputDirOption = Path("data/staged"),
    write_manifest: WriteManifestOption = True,
    docs_dir: DocsDirOption = None,
) -> None:
    report = validate_snapshot(input_dir)
    console.print(render_snapshot_validation_markdown(report))
    if write_manifest:
        manifest = build_snapshot_manifest(input_dir)
        manifest_path = input_dir / "snapshot_manifest.json"
        manifest_path.write_text(json_dumps(manifest), encoding="utf-8")
        console.print(f"Wrote snapshot manifest to {manifest_path}")
    if docs_dir is not None:
        paths = write_snapshot_docs(input_dir=input_dir, docs_dir=docs_dir)
        console.print(f"Wrote docs: {paths}")
    if report["status"] == "error":
        raise typer.Exit(code=1)


@app.command("snapshot-repair")
def snapshot_repair(
    config_dir: ConfigDirOption = Path("config"),
    input_dir: InputDirOption = Path("data/staged"),
    output_dir: OutputDirOption = Path("data/repaired"),
    docs_dir: DocsDirOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    summary = repair_snapshot(runtime=runtime, input_dir=input_dir, output_dir=output_dir)
    console.print(f"Repaired snapshot written to {output_dir}")
    console.print(
        "Repair summary: "
        f"{summary['repo_count']} repos, "
        f"{summary['final_repo_count']} final, "
        f"{summary['technology_edge_count']} normalized edges."
    )
    if summary["judge_changed_final_repo_ids"]:
        console.print(
            "Judge-adjusted final repos preserved: "
            f"{len(summary['judge_changed_final_repo_ids'])}"
        )
    if docs_dir is not None:
        paths = write_snapshot_docs(input_dir=output_dir, docs_dir=docs_dir)
        console.print(f"Wrote docs: {paths}")


@app.command("snapshot-refresh-contexts")
def snapshot_refresh_contexts(
    config_dir: ConfigDirOption = Path("config"),
    input_dir: InputDirOption = Path("data/staged"),
    output_dir: OutputDirOption = Path("data/refreshed"),
    repo: Annotated[list[str], typer.Option("--repo")] | None = None,
    min_ai_score: Annotated[int, typer.Option("--min-ai-score")] = 6,
    limit: OptionalIntOption = None,
    docs_dir: DocsDirOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
    require_github_token(runtime)
    summary = refresh_snapshot_contexts(
        runtime=runtime,
        input_dir=input_dir,
        output_dir=output_dir,
        repo_names=repo or None,
        min_ai_score=min_ai_score,
        limit=limit,
    )
    console.print(f"Refreshed snapshot written to {output_dir}")
    console.print(
        "Refresh summary: "
        f"{summary['refreshed_repo_count']} contexts refreshed, "
        f"{summary['final_repo_count']} final repos, "
        f"{summary['technology_edge_count']} normalized edges."
    )
    if docs_dir is not None:
        paths = write_snapshot_docs(input_dir=output_dir, docs_dir=docs_dir)
        console.print(f"Wrote docs: {paths}")


@app.command("snapshot-compare")
def snapshot_compare(
    left_dir: LeftDirOption,
    right_dir: RightDirOption,
) -> None:
    report = compare_snapshots(left_dir=left_dir, right_dir=right_dir)
    console.print(render_snapshot_comparison_markdown(report))


@app.command("experiment-log")
def experiment_log(
    left_dir: LeftDirOption,
    right_dir: RightDirOption,
    lever: Annotated[str, typer.Option("--lever")],
    decision: Annotated[str, typer.Option("--decision")] = "keep",
    files_changed: FilesChangedOption | None = None,
    note: Annotated[str | None, typer.Option("--note")] = None,
    branch_or_commit: Annotated[str | None, typer.Option("--branch-or-commit")] = None,
    evaluation_command: Annotated[str | None, typer.Option("--evaluation-command")] = None,
    ledger_path: LedgerPathOption = Path("experiments/ledger.jsonl"),
) -> None:
    report = compare_snapshots(left_dir=left_dir, right_dir=right_dir)
    entry = append_experiment_ledger_entry(
        ledger_path=ledger_path,
        report=report,
        lever=lever,
        files_changed=files_changed or [],
        decision=decision,
        note=note,
        branch_or_commit=branch_or_commit,
        evaluation_command=evaluation_command,
    )
    console.print(render_snapshot_comparison_markdown(report))
    console.print(f"Appended experiment entry to {ledger_path}: {entry['decision']}")


@app.command("snapshot-docs")
def snapshot_docs(
    input_dir: InputDirOption = Path("data/staged"),
    docs_dir: DocsDirRequiredOption = Path("docs"),
) -> None:
    paths = write_snapshot_docs(input_dir=input_dir, docs_dir=docs_dir)
    console.print(f"Wrote docs: {paths}")


def main() -> None:
    app()


def json_dumps(payload: object) -> str:
    import json

    return json.dumps(payload, indent=2, sort_keys=True)
