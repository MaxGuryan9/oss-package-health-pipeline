"""Run the Task 4 validate task on a temp copy of packages.duckdb, then break the copy and check it fails. 0 API requests.

Run from the project root:  python tests/validate_test.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main


def test_validate() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Work on a copy so the real packages.duckdb is never changed.
        main.DB_PATH = Path(tmp) / "test.duckdb"
        shutil.copy(main.PROJECT_DIR / "packages.duckdb", main.DB_PATH)

        # 1. The real data passes.
        main.validate()
        print("\n1. validate() passed on a copy of the real database")

        # 2. Delete one landed commit from raw_commits: the landed-vs-raw check must fail.
        with duckdb.connect(main.DB_PATH) as con:
            con.execute("DELETE FROM raw_commits WHERE id = (SELECT min(id) FROM raw_commits)")
        try:
            main.validate()
        except ValueError as e:
            message = str(e)
        else:
            raise AssertionError("validate() passed after a raw_commits row was deleted")
        print(f"2. validate() failed as expected:\n{message}")
        assert "missing from raw_commits" in message, "Failure did not name the missing raw_commits record"
        print("All validate checks passed")


if __name__ == "__main__":
    test_validate()
