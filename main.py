import json
import logging
import os
import shutil
import threading
import time
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
RATE_LIMIT_BUFFER = 25
RATE_LOCK = threading.Lock()  # shared by parallel repo fetches: one token, one budget

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"

logger = logging.getLogger("extract")


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


class TransientHTTPError(Exception):
    """Temporary failure (rate limit, 5xx, network, bad JSON); Prefect should retry."""


class PermanentHTTPError(Exception):
    """Failure a retry cannot fix (bad token, bad URL); Prefect should not retry."""


def wait_for_rate_limit(response: httpx.Response) -> None:
    """Primary limit: once remaining <= buffer, sleep until the reset time."""
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")
    if remaining is None or reset is None or int(remaining) > RATE_LIMIT_BUFFER:
        return
    with RATE_LOCK:
        delay = int(reset) + 5 - time.time()
        if delay > 0:
            logger.warning(f"Rate limit: {remaining} left; sleeping {delay:.0f}s until reset")
            time.sleep(delay)


def github_get(client: httpx.Client, url: str, params: dict | None = None) -> httpx.Response:
    """GET one URL. Returns a 200 response; raises Transient/PermanentHTTPError otherwise."""
    with RATE_LOCK:
        pass  # if another thread is sleeping off the rate limit, wait here until it's done
    try:
        response = client.get(url, params=params)
    except httpx.TransportError as exc:
        raise TransientHTTPError(f"Network error for {url}: {exc}") from exc

    wait_for_rate_limit(response)

    status = response.status_code
    if status == 200:
        return response
    if status in (403, 429) and "Retry-After" in response.headers:
        wait = int(response.headers["Retry-After"])
        with RATE_LOCK:
            time.sleep(wait)
        raise TransientHTTPError(f"Secondary rate limit {status} for {url}; waited {wait}s")
    if status == 429 or (status == 403 and "rate limit" in response.text.lower()):
        raise TransientHTTPError(f"Rate limit {status} for {url}: {response.text[:200]}")
    if status >= 500:
        raise TransientHTTPError(f"Server error {status} for {url}")
    raise PermanentHTTPError(f"HTTP {status} for {url}: {response.text[:200]}")


def landing_dir(repo: str, endpoint: str) -> Path:
    """data/raw/<owner>__<repo>/<endpoint>, per the README landing format."""
    return RAW_DIR / repo.replace("/", "__") / endpoint


def write_page(file_path: Path, body: dict) -> None:
    """Write one landing file atomically: temp file first, then rename."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(body, indent=2))
    os.replace(tmp_path, file_path)


def fetch_pages(
    client: httpx.Client,
    repo: str,
    endpoint: str,
    params: dict,
    path: str | None = None,
    out_dir: Path | None = None,
    stop_before: str | None = None,
) -> list[dict]:
    """Fetch every page of one endpoint, write each to a landing file, return all records.

    Resumes from pages already on disk: their records are read back and fetching
    continues from the last page's saved next_url (None means already complete).
    """
    out_dir = out_dir or landing_dir(repo, endpoint)
    for tmp in out_dir.glob("*.tmp"):
        tmp.unlink()  # leftovers from a crash mid-write; the matching .json was never created

    pages = sorted(out_dir.glob("page_*.json"))
    records = []
    for f in pages:
        records.extend(json.loads(f.read_text())["data"])

    if pages:
        url = json.loads(pages[-1].read_text())["next_url"]
        params = None
        page = len(pages) + 1
    else:
        url = API_BASE.format(REPOS=repo, TYPE=path or endpoint)
        params = {**params, "per_page": PER_PAGE}
        page = 1

    while url:
        response = github_get(client, url, params)
        try:
            data = response.json()
        except ValueError as exc:
            raise TransientHTTPError(f"Invalid JSON from {response.url}: {response.text[:200]!r}") from exc
        next_url = response.links.get("next", {}).get("url")
        if stop_before and data and data[-1]["updated_at"] < stop_before:
            next_url = None  # sorted by updated desc: every later page is older than the window
        write_page(out_dir / f"page_{page:04d}.json", {
            "repo": repo,
            "endpoint": endpoint,
            "url": str(response.url),
            "page": page,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "next_url": next_url,
            "data": data,
        })
        records.extend(data)
        url = next_url
        params = None
        page += 1
    return records


def fetch_reviews(
    client: httpx.Client,
    repo: str,
    prs: list[dict],
    created_since: str = WINDOW_START,
    out_dir: Path | None = None,
) -> list[dict]:
    """One reviews call per PR created in the window, merged or not (README Task 1)."""
    out_dir = out_dir or landing_dir(repo, "reviews")
    reviews = []
    for pr in prs:
        if pr["created_at"] >= created_since:
            n = pr["number"]
            reviews.extend(fetch_pages(
                client, repo, "reviews", {},
                path=f"pulls/{n}/reviews", out_dir=out_dir / f"pr_{n}",
            ))
    return reviews


def prepare_raw_dir() -> str:
    """README 'Resume or start fresh?': _LOADED present means data/raw is in DuckDB, so start over."""
    if LOADED_MARKER.exists():
        shutil.rmtree(RAW_DIR)
        return "fresh"
    return "resume"


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
