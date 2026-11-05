"""Task 2 test: load the first full run's landed files (archived) into a temp DuckDB twice; row counts must match.
Also checks one row per record, _load_state rows, and the _LOADED marker.
Costs 0 API requests. Evidence for the README (Task 2 Idempotent loads).

Run from the project root:  python tests/load_full_test.py
"""
import json
import logging
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

FULL_RUN = main.ARCHIVE_DIR / "raw_20261102T182509Z"  # files from the first full run (7,034 requests)

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


def table_counts() -> dict[str, int]:
    """Row count of every raw table in the test database."""
    with duckdb.connect(main.DB_PATH, read_only=True) as con:
        return {s["table"]: con.execute(f"SELECT count(*) FROM {s['table']}").fetchall()[0][0]
                for s in main.LOAD_SPECS.values()}


def test_load_full() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Copy the archived files (without their old _LOADED marker) and point main at the temp folder,
        # so the real data/ and packages.duckdb are only read, never written.
        shutil.copytree(FULL_RUN, Path(tmp) / "data" / "raw", ignore=shutil.ignore_patterns("_LOADED"))
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.DB_PATH = Path(tmp) / "test.duckdb"
        say(f"Source: {FULL_RUN}")

        main.load()
        first = table_counts()
        say(f"1. First load:  {first}")

        main.load()
        second = table_counts()
        say(f"2. Second load: {second}")
        assert first == second, "Row counts changed on the second load"
        say("Same files loaded twice: identical row counts")

        # One row per record: PR rows match the distinct PRs in the files
        # (a PR can sit on two pages if it was updated during pagination, so count distinct).
        pulls_on_disk = {(body["repo"], rec["number"])
                         for f in main.RAW_DIR.glob("*/pulls/page_*.json")
                         for body in [json.loads(f.read_text())] for rec in body["data"]}
        assert first["raw_pull_requests"] == len(pulls_on_disk), \
            f"raw_pull_requests has {first['raw_pull_requests']} rows, files hold {len(pulls_on_disk)} PRs"
        say(f"3. raw_pull_requests rows = distinct PRs in files ({len(pulls_on_disk)})")

        # _load_state: one row per repo and endpoint; _LOADED created in the temp folder only.
        with duckdb.connect(main.DB_PATH, read_only=True) as con:
            state = con.execute("SELECT count(*) FROM _load_state").fetchall()[0][0]
        assert state == len(main.REPOS) * len(main.LOAD_SPECS), f"Expected 12 _load_state rows, got {state}"
        assert main.LOADED_MARKER.exists(), "_LOADED marker was not created"
        say(f"4. _load_state has {state} rows (3 repos x 4 endpoints); _LOADED created")


if __name__ == "__main__":
    try:
        test_load_full()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
