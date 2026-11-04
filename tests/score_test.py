"""Run the Task 5 score_health task twice on a temp copy of packages.duckdb and check the table. 0 API requests.

Run from the project root:  python tests/score_test.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main


def test_score_health() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Work on a copy so the real packages.duckdb is never changed.
        main.DB_PATH = Path(tmp) / "test.duckdb"
        shutil.copy(main.PROJECT_DIR / "packages.duckdb", main.DB_PATH)

        main.score_health()
        with duckdb.connect(main.DB_PATH) as con:
            first = con.execute("SELECT * FROM health_scores ORDER BY rank").fetchall()

        # 1. One row per repo, ranks 1-3
        assert len(first) == len(main.REPOS), f"Expected {len(main.REPOS)} rows, got {len(first)}"
        assert [row[0] for row in first] == [1, 2, 3], "Ranks are not 1, 2, 3"
        print("1. health_scores has one row per repo, ranked 1-3")

        # 2. Score and every subscore are between 0 and 1
        for row in first:
            for value in row[2:]:
                assert 0 <= value <= 1, f"{row[1]}: value {value} outside 0-1"
        print("2. Score and all subscores are between 0 and 1")

        # 3. Running again gives the same table (idempotent)
        main.score_health()
        with duckdb.connect(main.DB_PATH) as con:
            second = con.execute("SELECT * FROM health_scores ORDER BY rank").fetchall()
        assert first == second, "Second run gave a different table"
        print("3. A second run gives the same table")
        print("All score checks passed")


if __name__ == "__main__":
    test_score_health()
