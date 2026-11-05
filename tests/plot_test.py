"""Run score_health and plot_health on a temp copy of packages.duckdb; check 3 non-empty PNGs. 0 API requests.

Run from the project root:  python tests/plot_test.py
"""
import logging
import shutil
import sys
import tempfile
from pathlib import Path

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


if __name__ == "__main__":
    try:
        test_plot_health()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
