# INTERVIEW: GOAL OF THIS TEST: check that Task 4 passes on good data AND fails on bad data. CONFIRMS: validate()
# passes on a copy of the real database; after one row is deleted from raw_commits it raises, and the message names
# the missing raw_commits record. CATCHES: a completeness check that can never fail, which would make a pass
# meaningless.
"""Run the Task 4 validate task on a temp copy of packages.duckdb, then break the copy and check it fails. 0 API requests.

Run from the project root:  python tests/validate_test.py
"""
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


# INTERVIEW: Works on a temp copy, so the row is deleted from the copy only. The landed files are still the real ones,
# so after the delete the files hold one commit the raw table lacks, which is exactly what the landed-vs-raw check
# must detect. `else: raise` fails the test if validate did not raise.
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


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_validate()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
