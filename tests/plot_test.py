# INTERVIEW: GOAL OF THIS TEST: check Task 6 produces its output. CONFIRMS: plot_health writes the three required PNGs
# and each is larger than 10 KB. CATCHES: a plot that crashes, is not saved, or is saved empty. It does not judge how
# a plot looks; that was reviewed by eye.
"""Run score_health and plot_health on a temp copy of packages.duckdb; check 3 non-empty PNGs. 0 API requests.

Run from the project root:  python tests/plot_test.py
"""
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


# INTERVIEW: Runs on a copy of packages.duckdb and a temp plots folder, so the real database and the committed plots
# are not changed. score_health runs first because plot_health reads the health_scores table.
def test_plot_health() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Work on a copy so the real packages.duckdb and plots/ are never changed.
        main.DB_PATH = Path(tmp) / "test.duckdb"
        main.PLOT_DIR = Path(tmp) / "plots"
        shutil.copy(main.PROJECT_DIR / "packages.duckdb", main.DB_PATH)

        main.score_health()  # plot_health reads health_scores
        main.plot_health()

        expected = ["dimension_profile.png", "monthly_trends.png", "load_vs_rejection.png"]
        for name in expected:
            path = main.PLOT_DIR / name
            assert path.exists(), f"{name} was not written"
            assert path.stat().st_size > 10_000, f"{name} is suspiciously small ({path.stat().st_size} bytes)"
            say(f"{name}: {path.stat().st_size:,} bytes")
        say("All plot checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_plot_health()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
