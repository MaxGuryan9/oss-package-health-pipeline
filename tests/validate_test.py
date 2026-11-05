"""Run the Task 4 validate task on a temp copy of packages.duckdb, then break the copy and check it fails. 0 API requests.

Run from the project root:  python tests/validate_test.py
"""
import logging
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

# Save this test's output to logs/test_logs/<this file's name>.log (overwritten each run, so the file always
# holds the latest run). Same handler pattern as main.py's per-task logs; main's task loggers also write here.
TEST_LOG_DIR = main.LOG_DIR / "test_logs"
TEST_LOG_DIR.mkdir(exist_ok=True)
test_handler = logging.FileHandler(TEST_LOG_DIR / f"{Path(__file__).stem}.log", mode="w")
test_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
for name in ["extract", "load", "transform", "validate", "model", "plot"]:
    logging.getLogger(name).addHandler(test_handler)
test_log = logging.getLogger(Path(__file__).stem)
test_log.setLevel(logging.INFO)
test_log.propagate = False  # say() already prints to the screen; don't print each line twice
test_log.addHandler(test_handler)


def say(message: str) -> None:
    """Print a result line and save it to this test's log."""
    print(message)
    test_log.info(message)


def test_validate() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Work on a copy so the real packages.duckdb is never changed.
        main.DB_PATH = Path(tmp) / "test.duckdb"
        shutil.copy(main.PROJECT_DIR / "packages.duckdb", main.DB_PATH)

        # 1. The real data passes.
        main.validate()
        say("\n1. validate() passed on a copy of the real database")

        # 2. Delete one landed commit from raw_commits: the landed-vs-raw check must fail.
        with duckdb.connect(main.DB_PATH) as con:
            con.execute("DELETE FROM raw_commits WHERE id = (SELECT min(id) FROM raw_commits)")
        try:
            main.validate()
        except ValueError as e:
            message = str(e)
        else:
            raise AssertionError("validate() passed after a raw_commits row was deleted")
        say(f"2. validate() failed as expected:\n{message}")
        assert "missing from raw_commits" in message, "Failure did not name the missing raw_commits record"
        say("All validate checks passed")


if __name__ == "__main__":
    try:
        test_validate()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
