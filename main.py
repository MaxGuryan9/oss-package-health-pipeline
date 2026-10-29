import os
from pathlib import Path

import httpx
from prefect import flow, get_run_logger, task

# --- Configuration ---
REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
WINDOW_START = "2024-01-01T00:00:00Z"
API_BASE = "https://api.github.com/repos/{REPOS}/{TYPE}"
PER_PAGE = 100
API_VERSION = "2026-03-10"
REQUEST_TIMEOUT = 30.0

RAW_DIR = Path("data/raw")
LOG_DIR = Path("logs")
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = Path("packages.duckdb")


def get_token() -> str:
    """Read the GitHub PAT from the GH_PAT environment variable."""
    token = os.environ.get("GH_PAT")
    if not token:
        raise RuntimeError("GH_PAT environment variable is not set")
    return token


HEADERS = {
    "Authorization": f"Bearer {get_token()}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": API_VERSION,
}


def make_client() -> httpx.Client:
    """HTTP client with GitHub auth and API headers set once."""
    return httpx.Client(headers=HEADERS, timeout=REQUEST_TIMEOUT)


@task(name="extract")
def extract() -> None:
    """Task 1: pull commits, PRs, reviews, and releases to data/raw/."""
    logger = get_run_logger()
    logger.info("Extract task started (not yet implemented)")


@flow(name="rhk2dx-package-health-flow")
def package_health_flow() -> None:
    extract()


if __name__ == "__main__":
    package_health_flow()
