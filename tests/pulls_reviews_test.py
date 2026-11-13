# INTERVIEW: GOAL OF THIS TEST: check the two parts of Task 1 that are easy to get wrong. CONFIRMS: pulls pagination
# stops at the first page older than the cutoff (pulls have no date filter), there is exactly one review folder per PR
# created since the cutoff and none for older PRs (Task 1 'Review calls'), and a rerun costs 0 requests. CATCHES:
# paging through years of old PRs, missing or extra review calls.
"""Manual check of the pulls stop date and per-PR review calls. Costs about 15-40 API requests.

Run from the project root:  python tests/pulls_reviews_test.py
"""
import json
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

CUTOFF = "2026-09-01T00:00:00Z"  # stands in for WINDOW_START so the test stays small
PULLS_PARAMS = {"state": "all", "sort": "updated", "direction": "desc"}

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


# INTERVIEW: Helper: runs the pulls fetch and then the review fetch, as fetch_repo does, and returns how many requests
# they used. Called twice so the first run and the rerun can be compared.
def fetch_both(pulls_dir: Path, reviews_dir: Path) -> tuple[list[dict], list[dict], int]:
    """Run the pulls fetch and the review fetch; return (prs, reviews, requests used)."""
    global REQUEST_COUNT
    REQUEST_COUNT = 0
    prs, _ = main.fetch_pages("dask/dask", "pulls", PULLS_PARAMS, out_dir=pulls_dir, stop_before=CUTOFF)
    reviews, _ = main.fetch_reviews("dask/dask", prs, created_since=CUTOFF, out_dir=reviews_dir)
    return prs, reviews, REQUEST_COUNT


# INTERVIEW: A September 2026 cutoff stands in for WINDOW_START to keep the test small. Every page except the last
# must end with a PR newer than the cutoff, and the last page must have next_url = null: together these show the fetch
# stopped exactly where it should.
def test_pulls_and_reviews() -> None:
    main.PER_PAGE = 10  # small pages force several pulls pages
    with tempfile.TemporaryDirectory() as tmp:
        pulls_dir = Path(tmp) / "pulls"
        reviews_dir = Path(tmp) / "reviews"
        prs, reviews, n = fetch_both(pulls_dir, reviews_dir)

        # 1. Pulls stop date.
        pages = [json.loads(f.read_text()) for f in sorted(pulls_dir.glob("page_*.json"))]
        say(f"1. Pulls: {len(prs)} PRs in {len(pages)} pages")
        for p in pages[:-1]:
            assert p["data"][-1]["updated_at"] >= CUTOFF, f"Page {p['page']} should have triggered the stop"
        assert pages[-1]["next_url"] is None, "Last pulls page should have next_url = null"
        last_updated = pages[-1]["data"][-1]["updated_at"] if pages[-1]["data"] else "n/a"
        say(f"   last PR on last page updated {last_updated} (cutoff {CUTOFF})")

        # 2. One review folder per PR created on or after the cutoff, none for older PRs.
        expected = {f"pr_{pr['number']}" for pr in prs if pr["created_at"] >= CUTOFF}
        folders = {d.name for d in reviews_dir.iterdir()} if reviews_dir.exists() else set()
        say(f"2. Reviews: {len(expected)} PRs created since cutoff, {len(folders)} review folders, "
              f"{len(reviews)} reviews")
        assert expected, "No PRs created since the cutoff; move CUTOFF earlier"
        assert folders == expected, f"Folders {sorted(folders)} != expected {sorted(expected)}"
        say(f"   total requests: {n}")

        # 3. Rerun: everything is on disk, so nothing is fetched.
        prs2, reviews2, n2 = fetch_both(pulls_dir, reviews_dir)
        say(f"3. Rerun: {len(prs2)} PRs, {len(reviews2)} reviews, {n2} requests")
        assert n2 == 0, f"Expected 0 requests on rerun, got {n2}"
        assert len(prs2) == len(prs) and len(reviews2) == len(reviews)

        say("All pulls/reviews checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_pulls_and_reviews()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
