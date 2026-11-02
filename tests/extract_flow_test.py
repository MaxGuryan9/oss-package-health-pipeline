"""Run the real extract subflow on a one-week window into a temp folder. Costs about 50-150 API requests.

Run from the project root:  python tests/extract_flow_test.py
"""
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

TEST_WINDOW = "2026-10-25T00:00:00Z"
ENDPOINTS = {"commits", "pulls", "releases", "reviews"}


def test_extract_flow() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point every path at the temp folder so nothing lands in the real data/raw or logs/.
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.LOG_DIR = Path(tmp) / "logs"

        first_run = main.extract(window_start=TEST_WINDOW)

        for repo in main.REPOS:
            repo_dir = main.RAW_DIR / repo.replace("/", "__")
            found = {d.name for d in repo_dir.iterdir()}
            assert found == ENDPOINTS, f"{repo}: folders {sorted(found)}, expected {sorted(ENDPOINTS)}"
        assert (main.LOG_DIR / "extract.log").exists(), "logs/extract.log was not written"

        rerun = main.extract(window_start=TEST_WINDOW)

        print(f"\nFirst run: {first_run} requests. Rerun: {rerun} requests.")
        print("Log file lines:", len((main.LOG_DIR / "extract.log").read_text().splitlines()))
        assert rerun == 0, f"Expected 0 requests on rerun, got {rerun}"
        print("All extract flow checks passed")


if __name__ == "__main__":
    test_extract_flow()
