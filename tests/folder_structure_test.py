# INTERVIEW: GOAL OF THIS TEST: check the landing format of Task 1 'Landing files'. CONFIRMS: files are named
# page_0001.json, page_0002.json, ... with no gaps, each has the README keys plus our next_url, the metadata matches,
# the records in the files add up to what fetch_pages returned, and no .tmp files remain. CATCHES: a wrong path or
# file name, a missing key, a lost record, a leftover temp file.
"""Manual check that fetch_pages writes landing files in the README format. Costs about 5 API requests.

Run from the project root:  python tests/folder_structure_test.py
Keep the files to inspect:  python tests/folder_structure_test.py --keep   (written to tests/output/)
"""
import json
import logging
import shutil
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

REQUIRED_KEYS = {"repo", "endpoint", "url", "page", "fetched_at", "next_url", "data"}
KEEP_DIR = Path(__file__).resolve().parent / "output"


# INTERVIEW: PER_PAGE is lowered to 10 so a small fetch still produces several files to check. The --keep option
# writes the files to tests/output/ (gitignored) so they can be opened and inspected by hand.
def test_folder_structure(keep: bool = False) -> None:
    # landing_dir builds the README path data/raw/<owner>__<repo>/<endpoint> (no API call).
    assert main.landing_dir("dask/dask", "commits") == main.RAW_DIR / "dask__dask" / "commits"

    main.PER_PAGE = 10  # small pages force several landing files
    with tempfile.TemporaryDirectory() as tmp:
        if keep:
            shutil.rmtree(KEEP_DIR, ignore_errors=True)  # clear the last run so old pages can't mix in
            out_dir = KEEP_DIR / "dask__dask" / "commits"
        else:
            out_dir = Path(tmp)
        records, _ = main.fetch_pages(
            "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=out_dir
        )

        files = sorted(out_dir.glob("page_*.json"))
        say(f"{len(records)} records in {len(files)} files: {files[0].name} .. {files[-1].name}")

        # One file per page, numbered page_0001, page_0002, ... with no gaps.
        expected = [f"page_{n:04d}.json" for n in range(1, len(files) + 1)]
        assert [f.name for f in files] == expected, f"Unexpected file names: {[f.name for f in files]}"

        # Every file has the README keys and matching metadata.
        total = 0
        for n, f in enumerate(files, start=1):
            body = json.loads(f.read_text())
            assert set(body) == REQUIRED_KEYS, f"{f.name} keys: {sorted(body)}"
            assert body["repo"] == "dask/dask" and body["endpoint"] == "commits" and body["page"] == n
            total += len(body["data"])
        assert total == len(records), f"Files hold {total} records, fetch_pages returned {len(records)}"

        # The rename step left no temp files behind.
        assert not list(out_dir.glob("*.tmp")), "Leftover .tmp files"

        say(f"page_0001.json url: {json.loads(files[0].read_text())['url']}")
        say("All landing-file checks passed")
        if keep:
            say(f"Files kept in {out_dir}")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_folder_structure(keep="--keep" in sys.argv)
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
