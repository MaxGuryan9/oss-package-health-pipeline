import json
import os
from datetime import datetime, timezone
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

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"


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


def github_get(client: httpx.Client, url: str, params: dict | None = None) -> httpx.Response:
    """GET one GitHub URL and return the response. Raises on any non-200 status."""
    response = client.get(url, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} for {url}: {response.text[:200]}")
    return response


def landing_dir(repo: str, endpoint: str) -> Path:
    """data/raw/<owner>__<repo>/<endpoint>, per the README landing format."""
    return RAW_DIR / repo.replace("/", "__") / endpoint


def write_page(file_path: Path, body: dict) -> None:
    """Write one landing file atomically: temp file first, then rename."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(body))
    os.replace(tmp_path, file_path)


def fetch_pages(
    client: httpx.Client,
    repo: str,
    endpoint: str,
    params: dict,
    path: str | None = None,
    out_dir: Path | None = None,
) -> list[dict]:
    """Fetch every page of one endpoint, write each to a landing file, return all records."""
    url = API_BASE.format(REPOS=repo, TYPE=path or endpoint)
    out_dir = out_dir or landing_dir(repo, endpoint)
    params = {**params, "per_page": PER_PAGE}
    records = []
    page = 1
    while url:
        response = github_get(client, url, params)
        data = response.json()
        write_page(out_dir / f"page_{page:04d}.json", {
            "repo": repo,
            "endpoint": endpoint,
            "url": str(response.url),
            "page": page,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": data,
        })
        records.extend(data)
        url = response.links.get("next", {}).get("url")
        params = None
        page += 1
    return records


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
