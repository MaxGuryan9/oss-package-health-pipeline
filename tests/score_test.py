"""Run the score_health task twice on a temp copy of packages.duckdb and check the table. 0 API requests.

Run from the project root:  python tests/score_test.py
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
        say("1. health_scores has one row per repo, ranked 1-3")

        # 2. Score and every subscore are between 0 and 1
        for row in first:
            for value in row[2:]:
                assert 0 <= value <= 1, f"{row[1]}: value {value} outside 0-1"
        say("2. Score and all subscores are between 0 and 1")

        # 3. Running again gives the same table (idempotent)
        main.score_health()
        with duckdb.connect(main.DB_PATH) as con:
            second = con.execute("SELECT * FROM health_scores ORDER BY rank").fetchall()
        assert first == second, "Second run gave a different table"
        say("3. A second run gives the same table")
        say("All score checks passed")


if __name__ == "__main__":
    try:
        test_score_health()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
