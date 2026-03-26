from __future__ import annotations

from pathlib import Path

from oss_ai_stack_map.cli import write_stage_timings
from oss_ai_stack_map.models.core import ClassificationDecision, StageTiming
from oss_ai_stack_map.storage.checkpoints import ClassificationCheckpointStore
from oss_ai_stack_map.storage.tables import read_parquet_models, write_rows


def test_checkpoint_store_records_interrupted_attempt(runtime_config, tmp_path: Path) -> None:
    runtime = runtime_config["runtime"]
    store = ClassificationCheckpointStore(tmp_path, write_csv=False)

    state = store.ensure_compatible_run(runtime=runtime, repo_ids=[1, 2, 3])
    state["attempt_started_at"] = "2026-03-25T00:00:00+00:00"
    state["processed_repo_count"] = 2
    state["stage"] = "classification_context_build"
    store.save_run_state(state)

    resumed = store.ensure_compatible_run(runtime=runtime, repo_ids=[1, 2, 3])
    timings = read_parquet_models(tmp_path / "checkpoints" / "stage_timings.parquet", StageTiming)

    assert resumed["attempt_id"] == "attempt-002"
    interrupted = next(t for t in timings if t.attempt_id == "attempt-001")
    assert interrupted.stage_id == "classification_attempt_total"
    assert interrupted.item_count == 2
    assert interrupted.notes == "interrupted during classification_context_build"


def test_write_stage_timings_merges_checkpoint_history(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    store = ClassificationCheckpointStore(output_dir, write_csv=False)
    store.write_stage_timings_checkpoint(
        [
            StageTiming(
                stage_id="classification_attempt_total",
                seconds=12.5,
                item_count=100,
                notes="completed attempt",
                attempt_id="attempt-001",
            )
        ]
    )

    write_stage_timings(
        output_dir=output_dir,
        timings=[
            StageTiming(
                stage_id="classification_total",
                seconds=1.25,
                item_count=0,
                attempt_id="attempt-002",
            )
        ],
        write_csv=False,
    )

    timings = read_parquet_models(output_dir / "stage_timings.parquet", StageTiming)
    assert {(t.stage_id, t.attempt_id) for t in timings} == {
        ("classification_attempt_total", "attempt-001"),
        ("classification_total", "attempt-002"),
    }


def test_read_checkpoint_models_for_repo_ids_filters_rows(tmp_path: Path) -> None:
    output_dir = tmp_path / "run"
    write_rows(
        output_dir / "checkpoints" / "repo_inclusion_decisions",
        "part-00001",
        [
            ClassificationDecision(
                repo_id=1,
                full_name="owner/repo-1",
                passed_candidate_filter=True,
                passed_serious_filter=True,
                passed_ai_relevance_filter=True,
                passed_major_filter=True,
                score_serious=5,
                score_ai=5,
            ).to_row(),
            ClassificationDecision(
                repo_id=2,
                full_name="owner/repo-2",
                passed_candidate_filter=True,
                passed_serious_filter=False,
                passed_ai_relevance_filter=False,
                passed_major_filter=False,
                score_serious=1,
                score_ai=1,
            ).to_row(),
        ],
        write_csv=False,
    )
    store = ClassificationCheckpointStore(output_dir, write_csv=False)

    rows = store.read_checkpoint_models_for_repo_ids(
        "repo_inclusion_decisions",
        ClassificationDecision,
        {2},
    )

    assert [row.repo_id for row in rows] == [2]
