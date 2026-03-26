from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import TypeVar

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


def write_rows(output_dir: Path, table_name: str, rows: list[dict], write_csv: bool = True) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{table_name}.parquet"
    csv_path = output_dir / f"{table_name}.csv" if write_csv else None
    write_rows_to_paths(rows=rows, parquet_path=parquet_path, csv_path=csv_path)


def write_rows_to_paths(
    rows: list[dict],
    *,
    parquet_path: Path,
    csv_path: Path | None = None,
) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    _write_parquet_atomic(table, parquet_path)
    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_rows = [_csv_safe_row(row) for row in rows]
        csv_table = pa.Table.from_pylist(csv_rows)
        _write_csv_atomic(csv_table, csv_path)


def read_parquet_models(path: Path, model_type: type[ModelT]) -> list[ModelT]:
    table = pq.read_table(path)
    rows = table.to_pylist()
    return [model_type.model_validate(row) for row in rows]


def read_parquet_rows(path: Path) -> list[dict]:
    table = pq.read_table(path)
    return table.to_pylist()


def _csv_safe_row(row: dict) -> dict:
    safe_row = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            safe_row[key] = json.dumps(value)
        else:
            safe_row[key] = value
    return safe_row


def _write_parquet_atomic(table: pa.Table, path: Path) -> None:
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    pq.write_table(table, tmp_path)
    tmp_path.replace(path)


def _write_csv_atomic(table: pa.Table, path: Path) -> None:
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    pacsv.write_csv(table, tmp_path)
    tmp_path.replace(path)
