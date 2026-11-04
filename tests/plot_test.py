"""Run score_health and plot_health on a temp copy of packages.duckdb; check 3 non-empty PNGs. 0 API requests.

Run from the project root:  python tests/plot_test.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main


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
            print(f"{name}: {path.stat().st_size:,} bytes")
        print("All plot checks passed")


if __name__ == "__main__":
    test_plot_health()
