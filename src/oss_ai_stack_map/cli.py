from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from oss_ai_stack_map.analysis.snapshot import (
    build_snapshot_manifest,
    compare_snapshots,
    render_snapshot_comparison_markdown,
    render_snapshot_validation_markdown,
    repair_snapshot,
    validate_snapshot,
    write_snapshot_docs,
)
from oss_ai_stack_map.config.loader import load_runtime
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import StageTiming
from oss_ai_stack_map.pipeline.classification import classify_candidates
from oss_ai_stack_map.pipeline.discovery import discover_candidates
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


def log_progress(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S", time.gmtime())
    console.print(f"[dim]{timestamp}[/dim] {message}")


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


@app.command()
def discover(
    config_dir: ConfigDirOption = Path("config"),
    output_dir: OutputDirOption = Path("data/staged"),
    max_pages_per_query: OptionalIntOption = None,
    max_repos: OptionalIntOption = None,
) -> None:
    runtime = load_runtime(config_dir=config_dir)
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
    input_dir: InputDirOption = Path("data/staged"),
    top_n: OptionalIntOption = 10,
) -> None:
    summary = build_report_summary(input_dir=input_dir, top_n=top_n or 10)

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


@app.command("snapshot-compare")
def snapshot_compare(
    left_dir: LeftDirOption,
    right_dir: RightDirOption,
) -> None:
    report = compare_snapshots(left_dir=left_dir, right_dir=right_dir)
    console.print(render_snapshot_comparison_markdown(report))


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
