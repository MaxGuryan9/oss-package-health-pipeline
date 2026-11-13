# INTERVIEW: GOAL OF THIS TEST: the evidence for Task 2 'Idempotent loads' on the full data set. CONFIRMS: loading the
# same 7,291 page files twice gives identical row counts, raw_pull_requests has one row per distinct PR, _load_state
# has 12 rows (3 repos x 4 endpoints), and _LOADED is created. CATCHES: duplicate rows from a blind insert, one row
# per page instead of per record, a missing state row or marker.
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

# INTERVIEW: The tests live in tests/ but main.py is one folder up, so the project root is added to Python's search
# path before `import main`. Importing main needs GH_PAT set, because main reads the token at import time.
# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

# INTERVIEW: Decision: the test loads the archived files of the first full run, which never change, instead of
# data/raw, whose contents depend on the latest incremental run. An earlier test that used data/raw failed for that
# reason and was merged into this one.
FULL_RUN = main.ARCHIVE_DIR / "raw_20261102T182509Z"  # files from the first full run (7,034 requests)

# INTERVIEW: Shared block, repeated in every test file (decision: no shared helper module, course style keeps code in
# single files). It saves this test's output to logs/test_logs/<file name>.log, opened with mode='w' so the file
# always holds the latest run and can be quoted as evidence in the README.
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


# INTERVIEW: Prints a result line and writes it to the test's log file, so what was on screen is also saved. propagate
# = False above stops each line from being printed a second time by the root logger.
def say(message: str) -> None:
    """Print a result line and save it to this test's log."""
    print(message)
    test_log.info(message)


# INTERVIEW: Helper: row count of each raw table in the temp database, called after each load so the two loads can be
# compared.
def table_counts() -> dict[str, int]:
    """Row count of every raw table in the test database."""
    with duckdb.connect(main.DB_PATH, read_only=True) as con:
        return {s["table"]: con.execute(f"SELECT count(*) FROM {s['table']}").fetchall()[0][0]
                for s in main.LOAD_SPECS.values()}


# INTERVIEW: The archive is copied into a temp folder without its old _LOADED marker, and main's paths are pointed
# there, so the real data and database are only read. PRs are counted as distinct (repo, number) pairs because a PR
# updated during pagination can appear on two pages.
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


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_load_full()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
