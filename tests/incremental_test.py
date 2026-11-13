# INTERVIEW: GOAL OF THIS TEST: run extract -> load twice and prove the second run is incremental (Task 2 'Incremental
# loads'). CONFIRMS: the second run uses fewer requests, the commits request uses the _load_state watermark as
# `since`, no raw table loses rows, and the first run's files were archived, not deleted. CATCHES: a second run that
# re-downloads everything, a lost watermark, deleted files.
"""Run extract -> load twice in a temp folder and check the second run is incremental. Costs about 100-120 API requests.

Run from the project root:  python tests/incremental_test.py
"""
import json
import logging
import sys
import tempfile
from pathlib import Path

import duckdb

# INTERVIEW: The tests live in tests/ but main.py is one folder up, so the project root is added to Python's search
# path before `import main`. Importing main needs GH_PAT set, because main reads the token at import time.
# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

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

TEST_WINDOW = "2026-10-25T00:00:00Z"


# INTERVIEW: Helper: row count of each raw table in the temp database, taken after each run so the two runs can be
# compared. read_only=True is fine here because dbt is not running in this process.
def table_counts() -> dict:
    """Row count of every raw table in the test database."""
    counts = {}
    with duckdb.connect(main.DB_PATH, read_only=True) as con:
        for spec in main.LOAD_SPECS.values():
            counts[spec["table"]] = con.execute(f"SELECT count(*) FROM {spec['table']}").fetchall()[0][0]
    return counts


# INTERVIEW: Everything runs in a temp folder with its own database, so the real data and packages.duckdb are never
# touched. Row counts are checked with >= (not ==) because new commits or PRs can arrive on GitHub between the two
# runs.
def test_incremental() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point data paths at the temp folder so nothing touches the real data/ or packages.duckdb.
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.ARCHIVE_DIR = Path(tmp) / "data" / "archive"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.DB_PATH = Path(tmp) / "test.duckdb"

        # 1. Full run: no _load_state yet, so everything since TEST_WINDOW is fetched.
        first = main.extract(window_start=TEST_WINDOW)
        main.load()
        counts1 = table_counts()
        say(f"\n1. Full run:        {first} requests, {counts1}")

        # 2. Incremental run: _LOADED exists, so data/raw is archived and only records since the watermark are fetched.
        watermarks = main.read_watermarks()
        second = main.extract(window_start=TEST_WINDOW)
        main.load()
        counts2 = table_counts()
        say(f"2. Incremental run: {second} requests, {counts2}")

        # 3. The commits fetch used the watermark as `since` (date only: colons may be URL-encoded).
        since = watermarks[("scikit-learn/scikit-learn", "commits")]
        page1 = json.loads((main.RAW_DIR / "scikit-learn__scikit-learn" / "commits" / "page_0001.json").read_text())
        say(f"3. sklearn commits watermark {since}; first page URL {page1['url']}")
        assert since[:10] in page1["url"], "Commits fetch did not use the watermark as 'since'"
        assert second < first, f"Incremental run used {second} requests, full run used {first}"
        for table in counts1:
            assert counts2[table] >= counts1[table], f"{table} lost rows: {counts1[table]} -> {counts2[table]}"

        # 4. The first run's files were moved to the archive, not deleted.
        archived = list(main.ARCHIVE_DIR.glob("raw_*"))
        say(f"4. Archive: {[a.name for a in archived]}")
        assert len(archived) == 1, f"Expected 1 archived raw_* folder, found {len(archived)}"
        assert list(archived[0].rglob("page_*.json")), "Archived folder has no page files"
        say("All incremental checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_incremental()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
