# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.models.core import (
    DiscoveredRepo,
    JudgeDecision,
    RepoContext,
)
from oss_ai_stack_map.pipeline.classification import apply_judge_decisions, classify_repo
from oss_ai_stack_map.pipeline.normalize import build_repo_technology_edges, build_technology_rows
from oss_ai_stack_map.pipeline.reporting import build_report_summary
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows

REQUIRED_TABLES = [
    "repos.parquet",
    "repo_contexts.parquet",
    "repo_inclusion_decisions.parquet",
]
PASSTHROUGH_TABLE_STEMS = [
    "repos",
    "repo_contexts",
    "repo_dependency_evidence",
    "judge_decisions",
    "discovery_stage_timings",
    "classification_stage_timings",
    "stage_timings",
]
REGENERATED_TABLE_STEMS = [
    "repo_inclusion_decisions",
    "repo_technology_edges",
    "technologies",
]


def _load_rows(path: Path, columns: list[str] | None = None) -> list[dict[str, Any]]:
    return pq.read_table(path, columns=columns).to_pylist()


def _read_if_exists(path: Path, columns: list[str] | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return _load_rows(path, columns=columns)


def _row_count_if_exists(path: Path) -> int:
    if not path.exists():
        return 0
    return pq.read_metadata(path).num_rows


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _snapshot_date(input_dir: Path) -> str:
    repos_path = input_dir / "repos.parquet"
    if repos_path.exists():
        rows = pq.read_table(repos_path, columns=["snapshot_date"]).slice(0, 1).to_pylist()
        if rows:
            return str(rows[0]["snapshot_date"])
    return "unknown"


def _format_int(value: int) -> str:
    return f"{value:,}"


def _format_float(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value)):,}"
    return f"{value:,.2f}"


def _percent(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.0%"
    return f"{(100 * numerator / denominator):.1f}%"


def _percent_value(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(100 * numerator / denominator, 1)


def _percentile(values: list[int | float], p: float) -> float:
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


def _summarize(values: list[int | float]) -> dict[str, float]:
    return {
        "min": float(min(values)),
        "p25": _percentile(values, 0.25),
        "median": _percentile(values, 0.50),
        "p75": _percentile(values, 0.75),
        "mean": sum(values) / len(values),
        "max": float(max(values)),
    }


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _load_run_state(snapshot_dir: Path) -> dict[str, Any] | None:
    path = snapshot_dir / "checkpoints" / "run_state.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    tmp_path.replace(path)


def build_snapshot_manifest(snapshot_dir: Path) -> dict[str, Any]:
    snapshot_dir = snapshot_dir.resolve()
    files: list[dict[str, Any]] = []
    for path in sorted(snapshot_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(snapshot_dir).as_posix()
        entry: dict[str, Any] = {
            "path": relative,
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        if path.suffix == ".parquet":
            entry["row_count"] = pq.read_metadata(path).num_rows
        files.append(entry)

    metrics = snapshot_metrics(snapshot_dir)
    return {
        "generated_at": _utc_now(),
        "snapshot_dir": str(snapshot_dir),
        "snapshot_date": _snapshot_date(snapshot_dir),
        "metrics": metrics,
        "files": files,
    }


def snapshot_metrics(snapshot_dir: Path) -> dict[str, Any]:
    repos = _row_count_if_exists(snapshot_dir / "repos.parquet")
    contexts = _row_count_if_exists(snapshot_dir / "repo_contexts.parquet")
    decisions = _read_if_exists(
        snapshot_dir / "repo_inclusion_decisions.parquet",
        columns=[
            "repo_id",
            "passed_serious_filter",
            "passed_ai_relevance_filter",
            "passed_major_filter",
            "rule_passed_serious_filter",
            "rule_passed_ai_relevance_filter",
            "rule_passed_major_filter",
        ],
    )
    edge_repo_ids = {
        row["repo_id"]
        for row in _read_if_exists(snapshot_dir / "repo_technology_edges.parquet", columns=["repo_id"])
    }

    final_ids = {row["repo_id"] for row in decisions if row.get("passed_major_filter")}
    serious_ids = {row["repo_id"] for row in decisions if row.get("passed_serious_filter")}
    ai_ids = {row["repo_id"] for row in decisions if row.get("passed_ai_relevance_filter")}
    rule_flags_missing = sum(
        1
        for row in decisions
        if row.get("rule_passed_serious_filter") is None
        or row.get("rule_passed_ai_relevance_filter") is None
        or row.get("rule_passed_major_filter") is None
    )
    run_state = _load_run_state(snapshot_dir)
    edge_coverage = _edge_coverage_metrics(
        snapshot_dir=snapshot_dir,
        final_ids=final_ids,
        edge_repo_ids=edge_repo_ids,
    )

    return {
        "repos": repos,
        "repo_contexts": contexts,
        "repo_inclusion_decisions": len(decisions),
        "repo_technology_edges": _row_count_if_exists(snapshot_dir / "repo_technology_edges.parquet"),
        "technologies": _row_count_if_exists(snapshot_dir / "technologies.parquet"),
        "judge_decisions": _row_count_if_exists(snapshot_dir / "judge_decisions.parquet"),
        "serious_repos": len(serious_ids),
        "ai_relevant_repos": len(ai_ids),
        "final_repos": len(final_ids),
        "final_repos_with_edges": edge_coverage["final_repos_with_edges"],
        "final_repos_missing_edges": edge_coverage["final_repos_missing_edges"],
        "final_repos_with_only_unmapped_dependency_evidence": edge_coverage[
            "final_repos_with_only_unmapped_dependency_evidence"
        ],
        "final_repos_with_no_dependency_evidence": edge_coverage[
            "final_repos_with_no_dependency_evidence"
        ],
        "final_repos_with_mapped_dependency_evidence_but_no_edge": edge_coverage[
            "final_repos_with_mapped_dependency_evidence_but_no_edge"
        ],
        "rule_flag_missing_rows": rule_flags_missing,
        "run_state_status": run_state.get("status") if run_state else None,
    }


def _edge_coverage_metrics(
    *,
    snapshot_dir: Path,
    final_ids: set[int],
    edge_repo_ids: set[int] | None = None,
) -> dict[str, int]:
    edge_repo_ids = edge_repo_ids or {
        row["repo_id"]
        for row in _read_if_exists(snapshot_dir / "repo_technology_edges.parquet", columns=["repo_id"])
    }
    missing_edge_final_ids = final_ids - edge_repo_ids
    coverage = {
        "final_repos_with_edges": len(edge_repo_ids),
        "final_repos_missing_edges": len(missing_edge_final_ids),
        "final_repos_with_only_unmapped_dependency_evidence": 0,
        "final_repos_with_no_dependency_evidence": 0,
        "final_repos_with_mapped_dependency_evidence_but_no_edge": 0,
    }
    if not missing_edge_final_ids:
        return coverage

    evidence_path = snapshot_dir / "repo_dependency_evidence.parquet"
    if evidence_path.exists():
        rows = _read_if_exists(evidence_path, columns=["repo_id", "technology_id"])
        repo_ids_with_evidence = {
            row["repo_id"] for row in rows if row["repo_id"] in missing_edge_final_ids
        }
        repo_ids_with_mapped_evidence = {
            row["repo_id"]
            for row in rows
            if row["repo_id"] in missing_edge_final_ids and row.get("technology_id")
        }
    else:
        repo_ids_with_evidence = set()
        repo_ids_with_mapped_evidence = set()
        for row in _read_if_exists(snapshot_dir / "repo_contexts.parquet"):
            repo_id = row["repo_id"]
            if repo_id not in missing_edge_final_ids:
                continue
            deps = row.get("manifest_dependencies", []) + row.get("sbom_dependencies", []) + row.get(
                "import_dependencies", []
            )
            if deps:
                repo_ids_with_evidence.add(repo_id)
            if any(dep.get("technology_id") for dep in deps):
                repo_ids_with_mapped_evidence.add(repo_id)

    coverage["final_repos_with_mapped_dependency_evidence_but_no_edge"] = len(
        repo_ids_with_mapped_evidence
    )
    coverage["final_repos_with_only_unmapped_dependency_evidence"] = len(
        repo_ids_with_evidence - repo_ids_with_mapped_evidence
    )
    coverage["final_repos_with_no_dependency_evidence"] = len(
        missing_edge_final_ids - repo_ids_with_evidence
    )
    return coverage


def validate_snapshot(snapshot_dir: Path) -> dict[str, Any]:
    snapshot_dir = snapshot_dir.resolve()
    errors: list[str] = []
    warnings: list[str] = []

    missing = [name for name in REQUIRED_TABLES if not (snapshot_dir / name).exists()]
    if missing:
        errors.append(f"missing required tables: {', '.join(missing)}")

    if errors:
        return {
            "snapshot_dir": str(snapshot_dir),
            "status": "error",
            "errors": errors,
            "warnings": warnings,
            "metrics": {},
        }

    repos = _read_if_exists(snapshot_dir / "repos.parquet", columns=["repo_id"])
    contexts = _read_if_exists(snapshot_dir / "repo_contexts.parquet", columns=["repo_id"])
    decisions = _read_if_exists(
        snapshot_dir / "repo_inclusion_decisions.parquet",
        columns=[
            "repo_id",
            "passed_major_filter",
            "rule_passed_serious_filter",
            "rule_passed_ai_relevance_filter",
            "rule_passed_major_filter",
            "judge_applied",
        ],
    )
    edges = _read_if_exists(snapshot_dir / "repo_technology_edges.parquet", columns=["repo_id", "technology_id"])
    technologies = _read_if_exists(snapshot_dir / "technologies.parquet", columns=["technology_id"])
    judge_count = _row_count_if_exists(snapshot_dir / "judge_decisions.parquet")
    metrics = snapshot_metrics(snapshot_dir)

    repo_ids = {row["repo_id"] for row in repos}
    context_ids = {row["repo_id"] for row in contexts}
    decision_ids = {row["repo_id"] for row in decisions}
    edge_repo_ids = {row["repo_id"] for row in edges}
    technology_ids = {row["technology_id"] for row in technologies}
    edge_technology_ids = {row["technology_id"] for row in edges}
    final_ids = {row["repo_id"] for row in decisions if row.get("passed_major_filter")}

    if repo_ids != context_ids:
        errors.append("repo_contexts repo_id set does not match repos repo_id set")
    if repo_ids != decision_ids:
        errors.append("repo_inclusion_decisions repo_id set does not match repos repo_id set")
    if not edge_repo_ids.issubset(final_ids):
        errors.append("repo_technology_edges contains repo_ids not present in the final included set")
    if not edge_technology_ids.issubset(technology_ids):
        errors.append("repo_technology_edges contains technology_ids missing from technologies")

    if metrics["final_repos_with_mapped_dependency_evidence_but_no_edge"] > 0:
        warnings.append(
            f"{metrics['final_repos_with_mapped_dependency_evidence_but_no_edge']} final repos have mapped dependency evidence but no normalized technology edge"
        )
    if metrics["rule_flag_missing_rows"] > 0:
        warnings.append(
            f"{metrics['rule_flag_missing_rows']} decision rows are missing rule_passed_* provenance fields"
        )

    run_state = _load_run_state(snapshot_dir)
    if run_state and run_state.get("status") != "completed" and decisions:
        warnings.append(
            "checkpoint run_state.json is not completed even though materialized outputs exist"
        )
    if judge_count and sum(1 for row in decisions if row.get("judge_applied")) != judge_count:
        warnings.append("judge_decisions row count does not match judge_applied decision count")

    status = "error" if errors else "warning" if warnings else "ok"
    return {
        "snapshot_dir": str(snapshot_dir),
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "metrics": metrics,
    }


def render_snapshot_validation_markdown(report: dict[str, Any]) -> str:
    lines = ["# Snapshot Validation", ""]
    lines.append(f"Snapshot: `{report['snapshot_dir']}`")
    lines.append("")
    lines.append(f"Status: `{report['status']}`")
    lines.append("")
    metrics = report.get("metrics") or {}
    if metrics:
        lines.append(
            _markdown_table(
                ["Metric", "Value"],
                [
                    ["Repos", _format_int(metrics["repos"])],
                    ["Contexts", _format_int(metrics["repo_contexts"])],
                    ["Decisions", _format_int(metrics["repo_inclusion_decisions"])],
                    ["Final repos", _format_int(metrics["final_repos"])],
                    ["Final repos with edges", _format_int(metrics["final_repos_with_edges"])],
                    ["Final repos missing tracked edges", _format_int(metrics["final_repos_missing_edges"])],
                    [
                        "Missing tracked edges with only unmapped evidence",
                        _format_int(metrics["final_repos_with_only_unmapped_dependency_evidence"]),
                    ],
                    [
                        "Missing tracked edges with no dependency evidence",
                        _format_int(metrics["final_repos_with_no_dependency_evidence"]),
                    ],
                    ["Missing rule provenance rows", _format_int(metrics["rule_flag_missing_rows"])],
                    ["Run state status", str(metrics["run_state_status"])],
                ],
            )
        )
        lines.append("")
    if report["errors"]:
        lines.append("Errors:")
        lines.append("")
        for item in report["errors"]:
            lines.append(f"- {item}")
        lines.append("")
    if report["warnings"]:
        lines.append("Warnings:")
        lines.append("")
        for item in report["warnings"]:
            lines.append(f"- {item}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _copy_or_link(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _copy_passthrough_files(input_dir: Path, output_dir: Path) -> None:
    for stem in PASSTHROUGH_TABLE_STEMS:
        for suffix in (".parquet", ".csv"):
            src = input_dir / f"{stem}{suffix}"
            if src.exists():
                _copy_or_link(src, output_dir / src.name)


def _reconciled_run_state(
    *,
    input_dir: Path,
    output_dir: Path,
    repo_count: int,
) -> dict[str, Any]:
    state = _load_run_state(input_dir) or {}
    now = _utc_now()
    state.update(
        {
            "updated_at": now,
            "status": "completed",
            "stage": "completed",
            "processed_repo_count": repo_count,
            "remaining_repo_count": 0,
            "total_repos": repo_count,
            "repair_source_dir": str(input_dir.resolve()),
        }
    )
    if not state.get("command"):
        state["command"] = "repair"
    if not state.get("started_at"):
        state["started_at"] = now
    _write_json(output_dir / "checkpoints" / "run_state.json", state)
    return state


def _dependency_rows_from_contexts(
    *,
    contexts: list[RepoContext],
    repos_by_id: dict[int, DiscoveredRepo],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for context in contexts:
        repo = repos_by_id[context.repo_id]
        for dependency in (
            context.manifest_dependencies + context.sbom_dependencies + context.import_dependencies
        ):
            rows.append(
                dependency.to_row(
                    repo_id=context.repo_id,
                    snapshot_date=repo.snapshot_date,
                )
            )
    return rows


def repair_snapshot(
    *,
    runtime: RuntimeConfig,
    input_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    if input_dir == output_dir:
        raise ValueError("repair output_dir must be different from input_dir")

    repos = read_parquet_models(input_dir / "repos.parquet", DiscoveredRepo)
    contexts = read_parquet_models(input_dir / "repo_contexts.parquet", RepoContext)
    existing_judge_decisions = (
        read_parquet_models(input_dir / "judge_decisions.parquet", JudgeDecision)
        if (input_dir / "judge_decisions.parquet").exists()
        else []
    )

    context_by_repo = {context.repo_id: context for context in contexts}
    alias_lookup = runtime.aliases.alias_lookup()
    decisions = [
        classify_repo(
            runtime=runtime,
            repo=repo,
            context=context_by_repo[repo.repo_id],
            alias_lookup=alias_lookup,
        )
        for repo in repos
    ]
    if existing_judge_decisions:
        apply_judge_decisions(
            runtime=runtime,
            decisions=decisions,
            judge_decisions=existing_judge_decisions,
        )

    technology_edges = build_repo_technology_edges(
        runtime=runtime,
        contexts=contexts,
        decisions=decisions,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    _copy_passthrough_files(input_dir, output_dir)
    if (input_dir / "repo_dependency_evidence.parquet").exists():
        _copy_or_link(
            input_dir / "repo_dependency_evidence.parquet",
            output_dir / "repo_dependency_evidence.parquet",
        )
        csv_input = input_dir / "repo_dependency_evidence.csv"
        if csv_input.exists():
            _copy_or_link(csv_input, output_dir / "repo_dependency_evidence.csv")
    else:
        dependency_rows = _dependency_rows_from_contexts(
            contexts=contexts,
            repos_by_id={repo.repo_id: repo for repo in repos},
        )
        write_rows(
            output_dir,
            "repo_dependency_evidence",
            dependency_rows,
            write_csv=runtime.study.outputs.write_csv,
        )

    write_rows(
        output_dir,
        "repo_inclusion_decisions",
        [decision.to_row() for decision in decisions],
        write_csv=runtime.study.outputs.write_csv,
    )
    write_rows(
        output_dir,
        "repo_technology_edges",
        [edge.to_row() for edge in technology_edges],
        write_csv=runtime.study.outputs.write_csv,
    )
    write_rows(
        output_dir,
        "technologies",
        build_technology_rows(runtime),
        write_csv=runtime.study.outputs.write_csv,
    )
    if existing_judge_decisions:
        write_rows(
            output_dir,
            "judge_decisions",
            [decision.to_row() for decision in existing_judge_decisions],
            write_csv=runtime.study.outputs.write_csv,
        )

    _reconciled_run_state(input_dir=input_dir, output_dir=output_dir, repo_count=len(repos))
    manifest = build_snapshot_manifest(output_dir)
    _write_json(output_dir / "snapshot_manifest.json", manifest)
    validation = validate_snapshot(output_dir)
    _write_json(output_dir / "validation_report.json", validation)

    changed_final_repo_ids = sorted(
        row.repo_id
        for row in decisions
        if row.rule_passed_major_filter != row.passed_major_filter
    )
    summary = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "repo_count": len(repos),
        "final_repo_count": sum(1 for decision in decisions if decision.passed_major_filter),
        "technology_edge_count": len(technology_edges),
        "judge_decision_count": len(existing_judge_decisions),
        "judge_changed_final_repo_ids": changed_final_repo_ids,
    }
    _write_json(output_dir / "repair_summary.json", summary)
    return summary


def compare_snapshots(left_dir: Path, right_dir: Path) -> dict[str, Any]:
    left_dir = left_dir.resolve()
    right_dir = right_dir.resolve()

    left_metrics = snapshot_metrics(left_dir)
    right_metrics = snapshot_metrics(right_dir)

    left_decisions = {
        row["repo_id"]: row
        for row in _read_if_exists(
            left_dir / "repo_inclusion_decisions.parquet",
            columns=["repo_id", "passed_major_filter", "primary_segment"],
        )
    }
    right_decisions = {
        row["repo_id"]: row
        for row in _read_if_exists(
            right_dir / "repo_inclusion_decisions.parquet",
            columns=["repo_id", "passed_major_filter", "primary_segment"],
        )
    }
    left_edges = _read_if_exists(left_dir / "repo_technology_edges.parquet", columns=["repo_id", "technology_id"])
    right_edges = _read_if_exists(
        right_dir / "repo_technology_edges.parquet",
        columns=["repo_id", "technology_id"],
    )

    shared_repo_ids = sorted(set(left_decisions) & set(right_decisions))
    changed_final = [
        repo_id
        for repo_id in shared_repo_ids
        if left_decisions[repo_id].get("passed_major_filter")
        != right_decisions[repo_id].get("passed_major_filter")
    ]
    changed_segments = [
        repo_id
        for repo_id in shared_repo_ids
        if (left_decisions[repo_id].get("primary_segment") or "")
        != (right_decisions[repo_id].get("primary_segment") or "")
    ]

    left_final_ids = {repo_id for repo_id, row in left_decisions.items() if row.get("passed_major_filter")}
    right_final_ids = {
        repo_id for repo_id, row in right_decisions.items() if row.get("passed_major_filter")
    }
    left_edge_pairs = {(row["repo_id"], row["technology_id"]) for row in left_edges}
    right_edge_pairs = {(row["repo_id"], row["technology_id"]) for row in right_edges}

    return {
        "left_dir": str(left_dir),
        "right_dir": str(right_dir),
        "left_metrics": left_metrics,
        "right_metrics": right_metrics,
        "metric_deltas": {
            key: right_metrics.get(key, 0) - left_metrics.get(key, 0)
            for key in sorted(set(left_metrics) | set(right_metrics))
            if isinstance(left_metrics.get(key), int) and isinstance(right_metrics.get(key), int)
        },
        "changed_final_repo_ids": changed_final,
        "changed_segment_repo_ids": changed_segments,
        "added_final_repo_ids": sorted(right_final_ids - left_final_ids),
        "removed_final_repo_ids": sorted(left_final_ids - right_final_ids),
        "added_edge_pairs": sorted(right_edge_pairs - left_edge_pairs),
        "removed_edge_pairs": sorted(left_edge_pairs - right_edge_pairs),
    }


def render_snapshot_comparison_markdown(report: dict[str, Any]) -> str:
    left_metrics = report["left_metrics"]
    right_metrics = report["right_metrics"]
    metric_keys = [
        "repos",
        "repo_contexts",
        "repo_inclusion_decisions",
        "repo_technology_edges",
        "technologies",
        "judge_decisions",
        "serious_repos",
        "ai_relevant_repos",
        "final_repos",
        "final_repos_with_edges",
        "final_repos_missing_edges",
    ]
    lines = ["# Snapshot Comparison", ""]
    lines.append(f"Left: `{report['left_dir']}`")
    lines.append(f"Right: `{report['right_dir']}`")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Metric", "Left", "Right", "Delta"],
            [
                [
                    key,
                    _format_int(left_metrics.get(key, 0)),
                    _format_int(right_metrics.get(key, 0)),
                    _format_int(report["metric_deltas"].get(key, 0)),
                ]
                for key in metric_keys
            ],
        )
    )
    lines.append("")
    lines.append(f"- Changed final repo flags: `{len(report['changed_final_repo_ids'])}`")
    lines.append(f"- Changed primary segments: `{len(report['changed_segment_repo_ids'])}`")
    lines.append(f"- Added final repos: `{len(report['added_final_repo_ids'])}`")
    lines.append(f"- Removed final repos: `{len(report['removed_final_repo_ids'])}`")
    lines.append(f"- Added normalized edges: `{len(report['added_edge_pairs'])}`")
    lines.append(f"- Removed normalized edges: `{len(report['removed_edge_pairs'])}`")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_snapshot_summary_markdown(input_dir: Path) -> str:
    input_dir = input_dir.resolve()
    summary = build_report_summary(input_dir=input_dir, top_n=5)
    judge = _read_if_exists(input_dir / "judge_decisions.parquet")
    snapshot_date = _snapshot_date(input_dir)
    run_state = _load_run_state(input_dir)

    lines = [f"# Snapshot Summary: {snapshot_date}", ""]
    lines.append(f"This summary covers the snapshot in `{input_dir}`.")
    lines.append("")
    lines.append("## Headline Counts")
    lines.append("")
    lines.append(f"- Discovered repos: `{summary.total_repos}`")
    lines.append(f"- Serious repos: `{summary.serious_repos}`")
    lines.append(f"- AI-relevant repos: `{summary.ai_relevant_repos}`")
    lines.append(f"- Final repos: `{summary.final_repos}`")
    if judge:
        lines.append(f"- Judge-reviewed repos: `{len(judge)}`")
    lines.append("")
    lines.append("## Top Technology Signals")
    lines.append("")
    for row in summary.top_technologies:
        lines.append(
            f"- {row['display_name']}: `{row['repo_count']}` repos (`{row['repo_share']:.0%}`)"
        )
    lines.append("")
    lines.append("Top provider prevalence:")
    lines.append("")
    for row in summary.top_providers:
        lines.append(
            f"- {row['provider_id']}: `{row['repo_count']}` repos (`{row['repo_share']:.0%}`)"
        )
    if run_state:
        lines.append("")
        lines.append("## Snapshot State")
        lines.append("")
        lines.append(f"- Run state status: `{run_state.get('status')}`")
        lines.append(f"- Started at: `{run_state.get('started_at')}`")
        lines.append(f"- Updated at: `{run_state.get('updated_at')}`")
    return "\n".join(lines).rstrip() + "\n"


def render_descriptive_statistics_markdown(input_dir: Path) -> str:
    input_dir = input_dir.resolve()
    repos = _read_if_exists(input_dir / "repos.parquet")
    contexts = _read_if_exists(input_dir / "repo_contexts.parquet")
    decisions = _read_if_exists(input_dir / "repo_inclusion_decisions.parquet")
    edges = _read_if_exists(input_dir / "repo_technology_edges.parquet")
    technologies = {
        row["technology_id"]: row for row in _read_if_exists(input_dir / "technologies.parquet")
    }
    evidence = _read_if_exists(input_dir / "repo_dependency_evidence.parquet")
    judge = _read_if_exists(input_dir / "judge_decisions.parquet")

    repo_by_id = {row["repo_id"]: row for row in repos}
    final_ids = {row["repo_id"] for row in decisions if row.get("passed_major_filter")}
    serious_ids = {row["repo_id"] for row in decisions if row.get("passed_serious_filter")}
    ai_ids = {row["repo_id"] for row in decisions if row.get("passed_ai_relevance_filter")}
    final_contexts = [row for row in contexts if row["repo_id"] in final_ids]

    owner_all = Counter((row.get("owner_type") or "Unknown") for row in repos)
    owner_final = Counter((repo_by_id[repo_id].get("owner_type") or "Unknown") for repo_id in final_ids)
    lang_all = Counter((row.get("primary_language") or "Unknown") for row in repos)
    lang_final = Counter(
        (repo_by_id[repo_id].get("primary_language") or "Unknown") for repo_id in final_ids
    )
    license_final = Counter(
        (repo_by_id[repo_id].get("license_spdx") or "Unknown") for repo_id in final_ids
    )
    serious_scores = Counter(row["score_serious"] for row in decisions)
    ai_scores = Counter(row["score_ai"] for row in decisions)
    edge_categories = Counter((row.get("category_id") or "unknown") for row in edges)
    segment_final = Counter(
        (row.get("primary_segment") or "unassigned")
        for row in decisions
        if row.get("passed_major_filter")
    )
    top_technologies = Counter(row["technology_id"] for row in edges).most_common(10)
    edge_coverage = _edge_coverage_metrics(snapshot_dir=input_dir, final_ids=final_ids)

    lines = [f"# Descriptive Statistics: {_snapshot_date(input_dir)}", ""]
    lines.append(f"This report summarizes `{input_dir}` from staged Parquet outputs.")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Metric", "Count", "Share of discovered repos"],
            [
                ["Discovered repos", _format_int(len(repos)), "100.0%"],
                ["Serious repos", _format_int(len(serious_ids)), _percent(len(serious_ids), len(repos))],
                ["AI-relevant repos", _format_int(len(ai_ids)), _percent(len(ai_ids), len(repos))],
                ["Final repos", _format_int(len(final_ids)), _percent(len(final_ids), len(repos))],
                ["Judge-reviewed repos", _format_int(len(judge)), _percent(len(judge), len(repos))],
            ],
        )
    )
    lines.append("")
    lines.append("## Population profile")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Owner type", "All repos", "Final repos"],
            [
                [
                    owner_type,
                    f"{_format_int(owner_all[owner_type])} ({_percent(owner_all[owner_type], len(repos))})",
                    f"{_format_int(owner_final[owner_type])} ({_percent(owner_final[owner_type], len(final_ids))})",
                ]
                for owner_type in sorted(owner_all)
            ],
        )
    )
    lines.append("")
    lines.append(
        _markdown_table(
            ["Rank", "All repos", "Final repos"],
            [
                [
                    str(index),
                    f"{left_name}: {_format_int(left_count)} ({_percent(left_count, len(repos))})",
                    f"{right_name}: {_format_int(right_count)} ({_percent(right_count, len(final_ids))})",
                ]
                for index, ((left_name, left_count), (right_name, right_count)) in enumerate(
                    zip(lang_all.most_common(10), lang_final.most_common(10), strict=False),
                    start=1,
                )
            ],
        )
    )
    lines.append("")
    lines.append("Top licenses in the final set:")
    lines.append("")
    for name, count in license_final.most_common(5):
        lines.append(f"- `{name}`: `{count}`")
    lines.append("")

    lines.append("## Scores and evidence")
    lines.append("")
    lines.append(
        "- Serious score modes: "
        + ", ".join(f"`{score}` ({count})" for score, count in serious_scores.most_common(5))
    )
    lines.append(
        "- AI score modes: "
        + ", ".join(f"`{score}` ({count})" for score, count in ai_scores.most_common(5))
    )
    lines.append(
        f"- Repos with manifest paths: `{sum(1 for row in contexts if row.get('manifest_paths'))}` ({_percent(sum(1 for row in contexts if row.get('manifest_paths')), len(contexts))})"
    )
    lines.append(
        f"- Repos with SBOM dependencies: `{sum(1 for row in contexts if row.get('sbom_dependencies'))}` ({_percent(sum(1 for row in contexts if row.get('sbom_dependencies')), len(contexts))})"
    )
    lines.append(
        f"- Repos with import-derived dependencies: `{sum(1 for row in contexts if row.get('import_dependencies'))}` ({_percent(sum(1 for row in contexts if row.get('import_dependencies')), len(contexts))})"
    )
    lines.append(
        f"- Evidence rows mapped to a known technology: `{sum(1 for row in evidence if row.get('technology_id'))}` ({_percent(sum(1 for row in evidence if row.get('technology_id')), len(evidence))})"
    )
    lines.append("")

    lines.append("## Technology graph inputs")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Metric", "Value"],
            [
                ["Normalized technology edges", _format_int(len(edges))],
                ["Technologies in catalog", _format_int(len(technologies))],
                ["Final repos with at least one tracked edge", _format_int(edge_coverage["final_repos_with_edges"])],
                ["Final repos with no tracked edge", _format_int(edge_coverage["final_repos_missing_edges"])],
                [
                    "Missing tracked edges with only unmapped evidence",
                    _format_int(edge_coverage["final_repos_with_only_unmapped_dependency_evidence"]),
                ],
                [
                    "Missing tracked edges with no dependency evidence",
                    _format_int(edge_coverage["final_repos_with_no_dependency_evidence"]),
                ],
            ],
        )
    )
    lines.append("")
    lines.append("Top technologies in final repos:")
    lines.append("")
    lines.append(
        _markdown_table(
            ["Rank", "Technology", "Repos", "Share of final repos"],
            [
                [
                    str(index),
                    technologies[technology_id]["display_name"],
                    _format_int(count),
                    _percent(count, len(final_ids)),
                ]
                for index, (technology_id, count) in enumerate(top_technologies, start=1)
            ],
        )
    )
    lines.append("")
    lines.append("Edge category mix:")
    lines.append("")
    for category, count in edge_categories.most_common():
        lines.append(f"- `{category}`: `{count}` ({_percent(count, len(edges))})")
    lines.append("")
    lines.append("Final primary segment mix:")
    lines.append("")
    for segment, count in segment_final.most_common():
        lines.append(f"- `{segment}`: `{count}` ({_percent(count, len(final_ids))})")
    lines.append("")
    lines.append("Final repo stars summary:")
    lines.append("")
    star_summary = _summarize([repo_by_id[repo_id]["stars"] for repo_id in final_ids])
    lines.append(
        f"- min `{_format_float(star_summary['min'])}`, median `{_format_float(star_summary['median'])}`, "
        f"p75 `{_format_float(star_summary['p75'])}`, max `{_format_float(star_summary['max'])}`"
    )
    lines.append("")
    lines.append("Final repo contexts:")
    lines.append("")
    lines.append(
        f"- Final repos with manifest paths: `{sum(1 for row in final_contexts if row.get('manifest_paths'))}` ({_percent(sum(1 for row in final_contexts if row.get('manifest_paths')), len(final_contexts))})"
    )
    lines.append(
        f"- Final repos with SBOM dependencies: `{sum(1 for row in final_contexts if row.get('sbom_dependencies'))}` ({_percent(sum(1 for row in final_contexts if row.get('sbom_dependencies')), len(final_contexts))})"
    )
    return "\n".join(lines).rstrip() + "\n"


def write_snapshot_docs(
    *,
    input_dir: Path,
    docs_dir: Path,
) -> dict[str, str]:
    docs_dir = docs_dir.resolve()
    docs_dir.mkdir(parents=True, exist_ok=True)
    snapshot_date = _snapshot_date(input_dir)
    summary_path = docs_dir / f"run-{snapshot_date}-summary.md"
    descriptive_path = docs_dir / f"descriptive-statistics-{snapshot_date}.md"
    validation_path = docs_dir / f"snapshot-validation-{snapshot_date}.md"

    summary_path.write_text(render_snapshot_summary_markdown(input_dir), encoding="utf-8")
    descriptive_path.write_text(
        render_descriptive_statistics_markdown(input_dir),
        encoding="utf-8",
    )
    validation_path.write_text(
        render_snapshot_validation_markdown(validate_snapshot(input_dir)),
        encoding="utf-8",
    )
    return {
        "summary": str(summary_path),
        "descriptive": str(descriptive_path),
        "validation": str(validation_path),
    }
