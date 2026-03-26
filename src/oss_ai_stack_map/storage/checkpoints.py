from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from oss_ai_stack_map.config.loader import RuntimeConfig
from oss_ai_stack_map.models.core import StageTiming
from oss_ai_stack_map.storage.tables import (
    read_parquet_models,
    read_parquet_rows,
    write_rows_to_paths,
)


class ClassificationCheckpointStore:
    def __init__(self, output_dir: Path, write_csv: bool) -> None:
        self.output_dir = output_dir
        self.write_csv = write_csv
        self.root = output_dir / "checkpoints"
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_state_path = self.root / "run_state.json"

    def ensure_compatible_run(
        self,
        *,
        runtime: RuntimeConfig,
        repo_ids: list[int],
    ) -> dict[str, Any]:
        state = self.load_run_state()
        config_hash = self._config_hash(runtime)
        compatible_hashes = self._compatible_config_hashes(runtime)
        repo_ids_hash = self._repo_ids_hash(repo_ids)
        now = utc_now()

        if state is not None:
            if (
                state.get("config_hash") not in compatible_hashes
                or state.get("repo_ids_hash") != repo_ids_hash
            ):
                raise ValueError(
                    "Checkpoint state does not match the current config or input repo set. "
                    "Use a new output directory or clear checkpoints."
                )
            if state.get("status") == "running" and state.get("attempt_id"):
                interrupted_seconds = elapsed_seconds(
                    state.get("attempt_started_at"),
                    now,
                )
                self.write_stage_timings_checkpoint(
                    [
                        StageTiming(
                            stage_id="classification_attempt_total",
                            seconds=interrupted_seconds,
                            item_count=state.get("processed_repo_count"),
                            notes=f"interrupted during {state.get('stage')}",
                            attempt_id=state.get("attempt_id"),
                        )
                    ]
                )
            attempt_index = int(state.get("attempt_index", 0)) + 1
            state.update(
                {
                    "updated_at": now,
                    "status": "running",
                    "total_repos": len(repo_ids),
                    "checkpoint_batch_size": runtime.study.checkpoint_batch_size,
                    "config_hash": config_hash,
                    "attempt_index": attempt_index,
                    "attempt_id": f"attempt-{attempt_index:03d}",
                    "attempt_started_at": now,
                }
            )
        else:
            attempt_index = 1
            state = {
                "command": "classification",
                "snapshot_date": runtime.study.snapshot_date.isoformat(),
                "started_at": now,
                "updated_at": now,
                "status": "running",
                "stage": "classification_context_build",
                "total_repos": len(repo_ids),
                "processed_repo_count": 0,
                "remaining_repo_count": len(repo_ids),
                "completed_checkpoint_batches": 0,
                "checkpoint_batch_size": runtime.study.checkpoint_batch_size,
                "config_hash": config_hash,
                "repo_ids_hash": repo_ids_hash,
                "attempt_index": attempt_index,
                "attempt_id": f"attempt-{attempt_index:03d}",
                "attempt_started_at": now,
            }
        self.save_run_state(state)
        return state

    def load_run_state(self) -> dict[str, Any] | None:
        if not self.run_state_path.exists():
            return None
        with self.run_state_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_run_state(self, payload: dict[str, Any]) -> None:
        tmp_path = self.run_state_path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
        tmp_path.replace(self.run_state_path)

    def update_progress(
        self,
        *,
        processed_repo_count: int,
        total_repos: int,
        stage: str,
    ) -> None:
        state = self.load_run_state() or {}
        state.update(
            {
                "updated_at": utc_now(),
                "status": "running",
                "stage": stage,
                "processed_repo_count": processed_repo_count,
                "remaining_repo_count": max(total_repos - processed_repo_count, 0),
                "completed_checkpoint_batches": len(
                    self.list_part_paths("repo_inclusion_decisions")
                ),
            }
        )
        self.save_run_state(state)

    def mark_completed(self, *, processed_repo_count: int, total_repos: int) -> None:
        state = self.load_run_state() or {}
        now = utc_now()
        if state.get("attempt_id"):
            self.write_stage_timings_checkpoint(
                [
                    StageTiming(
                        stage_id="classification_attempt_total",
                        seconds=elapsed_seconds(state.get("attempt_started_at"), now),
                        item_count=processed_repo_count,
                        notes="completed attempt",
                        attempt_id=state.get("attempt_id"),
                    )
                ]
            )
        state.update(
            {
                "updated_at": now,
                "status": "completed",
                "stage": "completed",
                "processed_repo_count": processed_repo_count,
                "remaining_repo_count": max(total_repos - processed_repo_count, 0),
                "completed_checkpoint_batches": len(
                    self.list_part_paths("repo_inclusion_decisions")
                ),
            }
        )
        self.save_run_state(state)

    def next_batch_index(self) -> int:
        existing = self.list_part_paths("repo_inclusion_decisions")
        if not existing:
            return 1
        suffixes = [int(path.stem.split("-")[-1]) for path in existing]
        return max(suffixes) + 1

    def write_batch_rows(self, table_name: str, batch_index: int, rows: list[dict]) -> None:
        if not rows:
            return
        table_dir = self.root / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        stem = f"part-{batch_index:05d}"
        parquet_path = table_dir / f"{stem}.parquet"
        csv_path = table_dir / f"{stem}.csv" if self.write_csv else None
        write_rows_to_paths(rows=rows, parquet_path=parquet_path, csv_path=csv_path)

    def list_part_paths(self, table_name: str) -> list[Path]:
        table_dir = self.root / table_name
        if not table_dir.exists():
            return []
        return sorted(table_dir.glob("part-*.parquet"))

    def list_completed_part_paths(self, table_name: str) -> list[Path]:
        if table_name == "repo_inclusion_decisions":
            return self.list_part_paths(table_name)
        completed_stems = {path.stem for path in self.list_part_paths("repo_inclusion_decisions")}
        return [
            path for path in self.list_part_paths(table_name) if path.stem in completed_stems
        ]

    def load_completed_repo_ids(self) -> set[int]:
        repo_ids: set[int] = set()
        for path in self.list_part_paths("repo_inclusion_decisions"):
            for row in read_parquet_rows(path):
                repo_ids.add(int(row["repo_id"]))
        return repo_ids

    def read_checkpoint_models(self, table_name: str, model_type: type) -> list[Any]:
        models: list[Any] = []
        for path in self.list_completed_part_paths(table_name):
            models.extend(read_parquet_models(path, model_type))
        return models

    def read_checkpoint_models_for_repo_ids(
        self,
        table_name: str,
        model_type: type,
        repo_ids: set[int],
    ) -> list[Any]:
        if not repo_ids:
            return []
        models: list[Any] = []
        remaining = set(repo_ids)
        for path in self.list_completed_part_paths(table_name):
            for row in read_parquet_rows(path):
                repo_id = row.get("repo_id")
                if repo_id not in remaining:
                    continue
                models.append(model_type.model_validate(row))
                remaining.remove(repo_id)
                if not remaining:
                    return models
        return models

    def read_checkpoint_rows(self, table_name: str) -> list[dict]:
        rows: list[dict] = []
        for path in self.list_completed_part_paths(table_name):
            rows.extend(read_parquet_rows(path))
        return rows

    def write_stage_timings_checkpoint(self, timings: list[StageTiming]) -> None:
        if not timings:
            return
        parquet_path = self.root / "stage_timings.parquet"
        existing = read_parquet_models(parquet_path, StageTiming) if parquet_path.exists() else []
        rows = [timing.to_row() for timing in [*existing, *timings]]
        csv_path = self.root / "stage_timings.csv" if self.write_csv else None
        write_rows_to_paths(rows=rows, parquet_path=parquet_path, csv_path=csv_path)

    def _config_hash(self, runtime: RuntimeConfig) -> str:
        payload = runtime.model_dump(mode="json", exclude={"env"})
        payload["study"].pop("judge", None)
        payload["study"].pop("outputs", None)
        payload["study"].pop("http", None)
        payload["study"].pop("checkpoint_batch_size", None)
        return stable_hash(payload)

    def _compatible_config_hashes(self, runtime: RuntimeConfig) -> set[str]:
        hashes = {self._config_hash(runtime)}

        legacy_payload = runtime.model_dump(mode="json", exclude={"env"})
        hashes.add(stable_hash(legacy_payload))

        legacy_without_judge = deepcopy(legacy_payload)
        legacy_without_judge["study"]["judge"]["enabled"] = False
        hashes.add(stable_hash(legacy_without_judge))

        return hashes

    def _repo_ids_hash(self, repo_ids: list[int]) -> str:
        return stable_hash(repo_ids)


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def elapsed_seconds(started_at: str | None, ended_at: str | None) -> float:
    if not started_at or not ended_at:
        return 0.0
    return (
        datetime.fromisoformat(ended_at) - datetime.fromisoformat(started_at)
    ).total_seconds()
