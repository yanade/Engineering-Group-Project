from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

import pandas as pd

from loading.load_service import LoadService


def test_dim_snapshot_truncates_and_inserts(monkeypatch):
    # ---- FAKES (local to this file) ----
    @dataclass
    class FakeDB:
        executed_sql: List[str] = field(default_factory=list)
        executemany_calls: List[Dict[str, Any]] = field(default_factory=list)

        def execute(self, sql: str) -> None:
            self.executed_sql.append(sql)

        def executemany(self, sql: str, params: List[Any], chunk_size: int = 1000) -> None:
            self.executemany_calls.append({"sql": sql, "params": params, "chunk_size": chunk_size})

    @dataclass
    class FakeS3Api:
        # not needed for dim test (no checkpoints)
        pass

    @dataclass
    class FakeS3LoadingClient:
        s3: Any = field(default_factory=FakeS3Api)
        parquet: Dict[str, pd.DataFrame] = field(default_factory=dict)

        def list_parquet_keys(self, table: str) -> List[str]:
            return sorted([k for k in self.parquet if k.startswith(f"{table}/")])

        def read_parquet_to_df(self, key: str) -> pd.DataFrame:
            return self.parquet[key].copy()

    # ---- Arrange ----
    table = "dim_staff"
    fake_db = FakeDB()
    fake_s3 = FakeS3LoadingClient()
    fake_s3.parquet[f"{table}/part-000.parquet"] = pd.DataFrame(
        [{"staff_id": 1, "name": "A"}, {"staff_id": 2, "name": "B"}]
    )

    svc = LoadService(processed_bucket="fake-processed", db=fake_db)
    svc.s3_client = fake_s3

    # Needed because your code requires typed DDL in CREATE_TABLE_SQL
    monkeypatch.setattr(
        "loading.load_service.CREATE_TABLE_SQL",
        {table: f'CREATE TABLE IF NOT EXISTS "{table}" (staff_id INT, name TEXT);'},
        raising=True,
    )

    # ---- Act ----
    res = svc.load_one_table(table)

    # ---- Assert ----
    assert res["status"] == "loaded"
    assert res["mode"] == "snapshot"
    assert res["rows"] == 2

    assert any("CREATE TABLE IF NOT EXISTS" in s for s in fake_db.executed_sql)
    assert any(f'TRUNCATE TABLE "{table}"' in s for s in fake_db.executed_sql)

    assert len(fake_db.executemany_calls) == 1
    assert len(fake_db.executemany_calls[0]["params"]) == 2

