import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Route our standard-library "extract" and "load" loggers into Prefect's logs. Must be set before Prefect is imported.
os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "extract,load")

import duckdb
import httpx
from prefect import flow, task, unmapped
from prefect.cache_policies import NO_CACHE
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

# --- Configuration ---
REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
WINDOW_START = "2024-01-01T00:00:00Z"
API_BASE = "https://api.github.com/repos/{REPOS}/{TYPE}"
PER_PAGE = 100
API_VERSION = "2026-03-10"
REQUEST_TIMEOUT = 30.0
RATE_LIMIT_BUFFER = 25
RATE_LOCK = threading.Lock()  # shared by parallel repo fetches: one token, one budget
STOP = threading.Event()  # set on Ctrl-C so worker threads stop at their next request or sleep
PULLS_PARAMS = {"state": "all", "sort": "updated", "direction": "desc"}

# Task 2: one raw table per endpoint. "id" and "updated_at" are DuckDB expressions on each record (rec).
LOAD_SPECS = {
    "commits": {
        "table": "raw_commits", "files": "*/commits/page_*.json", "id_type": "VARCHAR",
        "id": "rec->>'sha'", "updated_at": "rec->'commit'->'committer'->>'date'",
    },
    "pulls": {
        "table": "raw_pull_requests", "files": "*/pulls/page_*.json", "id_type": "BIGINT",
        "id": "rec->>'number'", "updated_at": "rec->>'updated_at'",
    },
    "reviews": {
        "table": "raw_pr_reviews", "files": "*/reviews/pr_*/page_*.json", "id_type": "BIGINT",
        "id": "rec->>'id'", "updated_at": "rec->>'submitted_at'",
    },
    "releases": {
        "table": "raw_releases", "files": "*/releases/page_*.json", "id_type": "BIGINT",
        "id": "rec->>'id'", "updated_at": "rec->>'updated_at'",
    },
}

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"

logger = logging.getLogger("extract")
load_logger = logging.getLogger("load")


def setup_file_log(name: str) -> None:
    """Also write logger `name` to logs/<name>.log (README: each task gets its own log file)."""
    log = logging.getLogger(name)
    if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
        LOG_DIR.mkdir(exist_ok=True)
        handler = logging.FileHandler(LOG_DIR / f"{name}.log")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(handler)
    log.setLevel(logging.INFO)


# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": API_VERSION,
}


class TransientHTTPError(Exception):
    """Temporary failure (rate limit, 5xx, network, bad JSON); Prefect should retry."""


class PermanentHTTPError(Exception):
    """Failure a retry cannot fix (bad token, bad URL); Prefect should not retry."""


class RunInterrupted(Exception):
    """Raised in worker threads after Ctrl-C; never retried."""


def pause(seconds: float) -> None:
    """Sleep, but wake early and raise if the run is interrupted."""
    if STOP.wait(seconds):
        raise RunInterrupted("Stopped by Ctrl-C")


def github_get(url: str, params: dict | None = None) -> httpx.Response:
    """GET one URL and check quota. Returns a 200 response; raises Transient/PermanentHTTPError otherwise."""
    with RATE_LOCK:
        pass  # if another thread is sleeping off the rate limit, wait here until it's done
    if STOP.is_set():
        raise RunInterrupted("Stopped by Ctrl-C")
    try:
        response = httpx.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    except httpx.TransportError as exc:
        raise TransientHTTPError(f"Network error for {url}: {exc}") from exc

    # primary limit: once remaining <= buffer, sleep until the reset time (+5s margin)
    remaining = int(response.headers.get("X-RateLimit-Remaining", RATE_LIMIT_BUFFER + 1))
    reset = int(response.headers.get("X-RateLimit-Reset", time.time()))
    if remaining <= RATE_LIMIT_BUFFER:
        with RATE_LOCK:  # holding the lock pauses every other thread too
            wait = max(reset - time.time(), 0) + 5
            logger.warning(f"Rate limit: {remaining} left; sleeping {wait:.0f}s until reset")
            pause(wait)

    status = response.status_code
    if status == 200:
        return response
    # secondary limit: GitHub says how many seconds to wait
    if status in (403, 429) and "Retry-After" in response.headers:
        wait = int(response.headers["Retry-After"])
        with RATE_LOCK:
            pause(wait)
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
    repo: str,
    endpoint: str,
    params: dict,
    path: str | None = None,
    out_dir: Path | None = None,
    stop_before: str | None = None,
    quiet: bool = False,
) -> tuple[list[dict], int]:
    """Fetch every page of one endpoint, write each to a landing file. Returns (all records, requests made).

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

    if pages and not quiet:
        status = "already complete" if url is None else f"resuming at page {page}"
        logger.info(f"[{repo}] {endpoint}: {len(pages)} pages on disk, {status}")

    requests = 0
    while url:
        requests += 1
        response = github_get(url, params)
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
        if not quiet:
            remaining = response.headers.get("X-RateLimit-Remaining", "?")
            logger.info(f"[{repo}] {endpoint} page {page}: {len(data)} records (budget left {remaining})")
        records.extend(data)
        url = next_url
        params = None
        page += 1
    return records, requests


def fetch_reviews(
    repo: str,
    prs: list[dict],
    created_since: str = WINDOW_START,
    out_dir: Path | None = None,
) -> tuple[list[dict], int]:
    """One reviews call per PR created in the window, merged or not (README Task 1). Returns (reviews, requests)."""
    out_dir = out_dir or landing_dir(repo, "reviews")
    # Dict by number drops PRs listed twice (a resumed pulls fetch can repeat one at a page boundary).
    in_window = {pr["number"]: pr for pr in prs if pr["created_at"] >= created_since}
    total = len(in_window)
    reviews = []
    requests = 0
    for i, n in enumerate(in_window, start=1):
        records, used = fetch_pages(
            repo, "reviews", {}, path=f"pulls/{n}/reviews", out_dir=out_dir / f"pr_{n}", quiet=True,
        )
        reviews.extend(records)
        requests += used
        if i % 25 == 0 or i == total:
            logger.info(f"[{repo}] reviews: {i}/{total} PRs ({i / total:.0%})")
    return reviews, requests


def prepare_raw_dir() -> str:
    """README 'Resume or start fresh?': _LOADED present means data/raw is in DuckDB, so start over."""
    if LOADED_MARKER.exists():
        shutil.rmtree(RAW_DIR)
        return "fresh"
    return "resume"


def retry_if_transient(task, task_run, state) -> bool:
    """Prefect retry condition: retry everything except PermanentHTTPError and Ctrl-C stops."""
    try:
        state.result()
    except (PermanentHTTPError, RunInterrupted):
        return False
    except Exception:
        return True
    return True


RETRY_SETTINGS = {
    "retries": 5,
    "retry_delay_seconds": exponential_backoff(backoff_factor=10),  # 10, 20, 40, 80, 160 s
    "retry_jitter_factor": 0.5,
    "retry_condition_fn": retry_if_transient,
    "cache_policy": NO_CACHE,
}


@task(name="fetch-repo", task_run_name="{repo}", **RETRY_SETTINGS)
def fetch_repo(repo: str, window_start: str) -> int:
    """All four endpoints for one repo: pulls, then their reviews, then commits and releases. Returns requests made."""
    prs, n_pulls = fetch_pages(repo, "pulls", PULLS_PARAMS, stop_before=window_start)
    reviews, n_reviews = fetch_reviews(repo, prs, created_since=window_start)
    commits, n_commits = fetch_pages(repo, "commits", {"since": window_start})
    releases, n_releases = fetch_pages(repo, "releases", {})
    requests = n_pulls + n_reviews + n_commits + n_releases
    logger.info(f"[{repo}] pulls {len(prs)}, reviews {len(reviews)}, commits {len(commits)}, "
                f"releases {len(releases)}; {requests} requests")
    return requests


def records_sql(spec: dict) -> str:
    """SELECT one row per record from every landing file of one endpoint, in raw-table column order."""
    return f"""
        SELECT repo, {spec['id']} AS id, {spec['updated_at']} AS updated_at, fetched_at,
               filename AS source_file, rec AS payload
        FROM (SELECT repo, fetched_at, filename, unnest(data) AS rec
              FROM read_json('{RAW_DIR}/{spec['files']}',
                             columns = {{repo: 'VARCHAR', fetched_at: 'TIMESTAMP', data: 'JSON[]'}},
                             filename = true))
    """


def find_duplicates(con: duckdb.DuckDBPyConnection, spec: dict) -> list[tuple]:
    """(repo, id, copies, files) for every (repo, id) that appears more than once in the landing files."""
    return con.execute(f"""
        SELECT repo, id, count(*) AS copies, list(source_file ORDER BY source_file) AS files
        FROM ({records_sql(spec)})
        GROUP BY repo, id
        HAVING count(*) > 1
        ORDER BY repo, id
    """).fetchall()


def load_table(con: duckdb.DuckDBPyConnection, endpoint: str) -> int:
    """Upsert every landed record of one endpoint into its raw table. Returns the row count after."""
    spec = LOAD_SPECS[endpoint]
    table = spec["table"]
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            repo VARCHAR, id {spec['id_type']}, updated_at TIMESTAMP, fetched_at TIMESTAMP,
            source_file VARCHAR, payload JSON, PRIMARY KEY (repo, id)
        )
    """)
    before = con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
    if not any(RAW_DIR.glob(spec["files"])):
        load_logger.info(f"{table}: no landing files, {before} rows unchanged")
        return before

    duplicates = find_duplicates(con, spec)
    if duplicates:
        load_logger.warning(f"{table}: {len(duplicates)} keys appear more than once in data/raw (first 10 below)")
        for repo, key, copies, files in duplicates[:10]:
            load_logger.warning(f"  {repo} id={key} x{copies}: {', '.join(files)}")

    con.execute(f"INSERT OR REPLACE INTO {table} {records_sql(spec)}")
    after = con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
    load_logger.info(f"{table}: {before} rows before, {after} after")
    return after


def write_load_state(con: duckdb.DuckDBPyConnection) -> None:
    """One _load_state row per repo and endpoint: newest updated_at loaded, row count, load time."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS _load_state (
            repo VARCHAR, endpoint VARCHAR, max_updated_at TIMESTAMP, row_count BIGINT,
            loaded_at TIMESTAMP, PRIMARY KEY (repo, endpoint)
        )
    """)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)  # UTC, like the GitHub timestamps
    for endpoint, spec in LOAD_SPECS.items():
        con.execute(f"""
            INSERT OR REPLACE INTO _load_state
            SELECT repo, '{endpoint}', max(updated_at), count(*), ? FROM {spec['table']} GROUP BY repo
        """, [loaded_at])


@task(name="load-table", task_run_name="load {endpoint}", cache_policy=NO_CACHE)
def load_table_task(endpoint: str) -> int:
    with duckdb.connect(DB_PATH) as con:
        return load_table(con, endpoint)


@task(name="finish-load", cache_policy=NO_CACHE)
def finish_load_task() -> None:
    """README Task 2: update _load_state only after every table loaded, then create the _LOADED marker."""
    with duckdb.connect(DB_PATH) as con:
        write_load_state(con)
    LOADED_MARKER.touch()
    load_logger.info(f"_load_state updated; created {LOADED_MARKER}")


@flow(name="extract", task_runner=ThreadPoolTaskRunner(max_workers=len(REPOS)))
def extract(window_start: str = WINDOW_START) -> int:
    """Task 1: pull commits, PRs, reviews, and releases for all repos to data/raw/. Returns requests made."""
    STOP.clear()
    setup_file_log("extract")
    mode = prepare_raw_dir()
    logger.info(f"Extract starting ({mode}): {len(REPOS)} repos, window from {window_start}")
    try:
        # one task per repo, run in parallel; .result() raises if any repo failed after its retries
        counts = fetch_repo.map(REPOS, unmapped(window_start)).result()
    except KeyboardInterrupt:
        STOP.set()
        logger.warning("Ctrl-C received: stopping workers; pages on disk are kept for resume")
        raise
    total = sum(counts)
    logger.info(f"Extract finished: {total} API requests this run")
    return total


@flow(name="load")
def load() -> None:
    """Task 2: upsert data/raw/ into raw_* tables in packages.duckdb. Never calls the API."""
    setup_file_log("load")
    load_logger.info(f"Load starting: {RAW_DIR} -> {DB_PATH}")
    for endpoint in LOAD_SPECS:  # one at a time: DuckDB allows a single writer
        load_table_task(endpoint)
    finish_load_task()


@flow(name="rhk2dx-package-health-flow")
def package_health_flow() -> None:
    extract()


if __name__ == "__main__":
    package_health_flow()
