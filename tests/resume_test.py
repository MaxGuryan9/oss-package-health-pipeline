# INTERVIEW: GOAL OF THIS TEST: check Task 1 'Resumability' at the level of one endpoint, including the half-written
# page case the rubric describes. CONFIRMS: after deleting the last two pages and leaving a broken .tmp file, a rerun
# fetches exactly those two pages, returns the same records in the same order, removes the .tmp, and a further rerun
# costs 0 requests. CATCHES: starting over, trusting a half-written file, duplicated or missing records after a
# resume.
"""Manual check that fetch_pages resumes from pages on disk. Costs about 7 API requests.

Run from the project root:  python tests/resume_test.py
"""
import logging
import sys
import tempfile
from pathlib import Path

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

REQUEST_COUNT = 0
_real_github_get = main.github_get


# INTERVIEW: Counts API requests without changing main.py: it wraps the real github_get and replaces main.github_get
# with the wrapper. This works because fetch_pages looks up github_get by name each time it is called.
def counting_github_get(*args, **kwargs):
    """Wrap main.github_get to count API requests."""
    global REQUEST_COUNT
    REQUEST_COUNT += 1
    return _real_github_get(*args, **kwargs)


main.github_get = counting_github_get


# INTERVIEW: Helper: resets the request counter, runs fetch_pages on the same folder, and returns the records with the
# number of requests used. Called three times: full fetch, resume, and rerun.
def fetch(out_dir: Path) -> tuple[list[dict], int]:
    """Run fetch_pages and return (records, requests used)."""
    global REQUEST_COUNT
    REQUEST_COUNT = 0
    records, _ = main.fetch_pages(
        "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=out_dir
    )
    return records, REQUEST_COUNT


# INTERVIEW: The crash is simulated by deleting page files and writing a truncated .tmp, which is the state a real
# crash during write_page would leave. Comparing the lists of commit SHAs checks both that nothing is missing and that
# the order is unchanged.
def test_resume() -> None:
    main.PER_PAGE = 10  # small pages force several landing files
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)

        # 1. Full fetch.
        full, n = fetch(out_dir)
        pages = len(list(out_dir.glob("page_*.json")))
        say(f"1. Full fetch:      {len(full)} records, {pages} pages, {n} requests")
        assert n == pages

        # 2. Simulate a crash: last 2 pages missing, one half-written temp file left behind.
        (out_dir / f"page_{pages:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.tmp").write_text('{"repo": "dask/da')
        say(f"2. Simulated crash: deleted pages {pages - 1}-{pages}, wrote a half-written .tmp")

        # 3. Resume: only the 2 missing pages are fetched.
        resumed, n = fetch(out_dir)
        say(f"3. Resume:          {len(resumed)} records, {n} requests")
        assert n == 2, f"Expected 2 requests on resume, got {n}"
        assert [r["sha"] for r in resumed] == [r["sha"] for r in full], "Resumed records differ"
        assert len(list(out_dir.glob("page_*.json"))) == pages
        assert not list(out_dir.glob("*.tmp")), "Leftover .tmp file"

        # 4. Rerun on a finished endpoint: nothing to fetch.
        again, n = fetch(out_dir)
        say(f"4. Rerun complete:  {len(again)} records, {n} requests")
        assert n == 0, f"Expected 0 requests on a finished endpoint, got {n}"
        assert len(again) == len(full)

        say("All resume checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_resume()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
