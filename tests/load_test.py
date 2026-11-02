"""Load dask's landed files into a temp DuckDB twice and check the loads are idempotent. Costs 0 API requests.

Run from the project root:  python tests/load_test.py
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

REAL_MARKER = main.LOADED_MARKER


def table_counts() -> dict[str, int]:
    """Row count of every raw table in the test database."""
    with duckdb.connect(main.DB_PATH, read_only=True) as con:
        return {s["table"]: con.execute(f"SELECT count(*) FROM {s['table']}").fetchall()[0][0]
                for s in main.LOAD_SPECS.values()}


def test_load() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Copy one repo's real landing files, then point every path at the temp folder.
        # The real data/raw is only read, never written, so no _LOADED marker lands there.
        shutil.copytree(main.RAW_DIR / "dask__dask", Path(tmp) / "data" / "raw" / "dask__dask")
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.LOG_DIR = Path(tmp) / "logs"
        main.DB_PATH = Path(tmp) / "test.duckdb"

        # 1. One row per record: table rows match the records in the files.
        main.load()
        first = table_counts()
        print(f"\n1. First load:  {first}")
        pulls_on_disk = sum(len(json.loads(f.read_text())["data"])
                            for f in (main.RAW_DIR / "dask__dask" / "pulls").glob("page_*.json"))
        assert first["raw_pull_requests"] == pulls_on_disk, \
            f"raw_pull_requests has {first['raw_pull_requests']} rows, files hold {pulls_on_disk} PRs"

        # 2. Idempotent: loading the same files again changes nothing.
        main.load()
        second = table_counts()
        print(f"2. Second load: {second}")
        assert first == second, "Row counts changed on the second load"

        # 3. _load_state has one row per endpoint for dask, and the marker was created in the temp folder.
        with duckdb.connect(main.DB_PATH, read_only=True) as con:
            state = con.execute("SELECT repo, endpoint, max_updated_at, row_count FROM _load_state "
                                "ORDER BY endpoint").fetchall()
        print("3. _load_state:")
        for row in state:
            print("  ", row)
        assert len(state) == len(main.LOAD_SPECS), f"Expected {len(main.LOAD_SPECS)} _load_state rows"
        assert main.LOADED_MARKER.exists(), "_LOADED marker was not created"
        assert not REAL_MARKER.exists(), "Test created _LOADED in the real data/raw"

        log_text = (main.LOG_DIR / "load.log").read_text()
        assert "rows before" in log_text, "Row counts missing from load.log"
        print("All load checks passed")


if __name__ == "__main__":
    test_load()
