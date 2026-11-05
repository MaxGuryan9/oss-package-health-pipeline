import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Route our standard-library "extract", "load" and "transform" loggers into Prefect's logs. Must be set before Prefect is imported.
os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "extract,load,transform,validate,model,plot")

import duckdb
import httpx
import matplotlib
# Technical setting only, NOT part of the analysis: "Agg" makes matplotlib write PNG files without opening a window.
# Without it, macOS raises "Cannot create a GUI FigureManager outside the main thread" when a plot is drawn off the
# main thread (Prefect can run tasks in worker threads). It changes nothing about how the plots look.
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from prefect import flow, task, unmapped
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# --- Configuration ---
REPOS = ["scikit-learn/scikit-learn", "dask/dask", "plotly/plotly.py"]
WINDOW_START = "2024-01-01T00:00:00Z"
GIT_URL = "https://github.com/MaxGuryan9/package-health-project-ds3022.git"
WORK_POOL = "uvasds-k8s-pool"  # class Kubernetes pool (DEPLOYMENTS.md: keep as written)
API_BASE = "https://api.github.com/repos/{REPOS}/{TYPE}"
PER_PAGE = 100
API_VERSION = "2026-03-10"
REQUEST_TIMEOUT = 30.0
RATE_LIMIT_BUFFER = 25
RATE_LOCK = threading.Lock()  # shared by parallel repo fetches: one token, one budget
STOP = threading.Event()  # set on Ctrl-C so worker threads stop at their next request or sleep
API_REQUESTS = 0  # odometer: GitHub API requests sent this flow run, across all threads and retries
ODOMETER_LOCK = threading.Lock()  # separate from RATE_LOCK, which is held during rate-limit sleeps
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

# Task 4: thresholds for outlier detection (README Task 4). Different author types get different cutoffs,
# because a wait that is normal for a human PR means an automated PR got stuck.
HUMAN_MERGE_QUANTILE = 0.99  # human PRs: slower than 99% of that repo's merged human PRs; 0.99 (not 0.90) so every flag is far outside the trend
BOT_MERGE_HOURS = 7 * 24     # bot PRs: automated updates normally merge within hours, so a week means stuck
COMMIT_BURST_IQR = 3         # weekly commits above Q3 + 3 x IQR are "far outside" (Tukey's far-out rule)

# Task 4: staging rows dropped on purpose (raw count minus these must equal the staging count)
STAGING_SPECS = {
    "commits": {"model": "stg_commits", "dropped": "false"},
    "pulls": {"model": "stg_pull_requests", "dropped": "false"},
    "reviews": {"model": "stg_pr_reviews", "dropped": "payload->>'state' = 'PENDING'"},
    "releases": {"model": "stg_releases", "dropped": "(payload->>'draft')::boolean"},
}

# Task 5: fixed-range min-max scoring. score = (value - zero_at) / (one_at - zero_at), clipped to 0-1.
# zero_at scores 0 (least healthy), one_at scores 1 (healthiest). Lower-is-better metrics have zero_at > one_at,
# so the same formula flips them. Fixed bounds (not the 3 repos' min and max) keep close repos close
# and do not change if the repo set changes.
# Weights sum to 100, in three tiers ranked by how directly each metric measures health and how strong
# its caveats are; equal within a tier.
SCORE_SPECS = {
    # Tier 1 (25 each): clear signals of maintainers keeping up with incoming work
    "pct_reviewed_within_7d": {"dimension": "responsiveness",  "weight": 25, "zero_at": 0,     "one_at": 1},
    "pct_open_stale":         {"dimension": "backlog",         "weight": 25, "zero_at": 1,     "one_at": 0},
    # Tier 2 (15 each): important idea, weaker metric (measures change, not level; regular releases are not always better)
    "top_author_share_trend": {"dimension": "contributors",    "weight": 15, "zero_at": 0.02,  "one_at": -0.02},  # +/- 2 pts per month
    "release_gap_cv":         {"dimension": "release_cadence", "weight": 15, "zero_at": 2,     "one_at": 0},      # CV 2 = gaps vary by twice their average
    # Tier 3 (10 each): can move for reasons other than health (commit habits; strict review lowers merge rate,
    # and a very high merge rate can mean weak review)
    "commit_trend":           {"dimension": "activity",        "weight": 10, "zero_at": -0.10, "one_at": 0.10},   # +/- 10% per month
    "merge_rate":             {"dimension": "throughput",      "weight": 10, "zero_at": 0,     "one_at": 1},
}

# Task 6: one fixed color per repo across every plot (README Presentation criterion)
REPO_COLORS = {
    "scikit-learn/scikit-learn": "#2a78d6",
    "dask/dask": "#eb6834",
    "plotly/plotly.py": "#2f9e6e",
}
TREND_PANELS = [
    # rows ordered by Task 5 weight tier (SCORE_SPECS); backlog and release cadence have no monthly column in mart_repo_monthly
    # (column, title, y-axis unit, how to summarize the window in the panel subtitle, tier and dimension)
    ("median_first_review_hours", "Median first-review time", "hours (log scale)", "median", "Tier 1 · Responsiveness"),
    ("active_contributors", "Active PR authors", "authors / month", "median", "Tier 2 · Contributors"),
    ("top_author_share", "Top author's share", "share of merged PRs (0-1)", "median", "Tier 2 · Contributors"),
    ("commits", "Commits", "commits / month", "total", "Tier 3 · Activity"),
    ("prs_opened", "PRs opened", "PRs / month", "total", "Tier 3 · Throughput"),
    ("prs_merged", "PRs merged", "PRs / month", "total", "Tier 3 · Throughput"),
]
# Look of images/commit_activity.png: matplotlib's default font, no box, light grid, gray tick labels, no legend frame
EXAMPLE_STYLE = {
    "font.family": "DejaVu Sans",
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False, "axes.spines.bottom": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#e5e5e5",
    "axes.facecolor": "white", "figure.facecolor": "white",
    "xtick.color": "#777777", "ytick.color": "#777777", "axes.labelcolor": "#777777",
    "xtick.major.size": 0, "ytick.major.size": 0,
    "legend.frameon": False,
}
# Task 6 plot 3: months grouped by PR load relative to the repo's own average (cutoffs 0.8x and 1.2x)
LOAD_BINS_SQL = """
    WITH months AS (
        SELECT repo, merge_ratio, active_contributors,
               prs_opened / avg(prs_opened) OVER (PARTITION BY repo) AS load_vs_average
        FROM mart_repo_monthly
        WHERE NOT is_partial_month AND merge_ratio IS NOT NULL
    )
    SELECT repo,
           CASE WHEN load_vs_average < 0.8 THEN 0 WHEN load_vs_average <= 1.2 THEN 1 ELSE 2 END AS load_bin,
           count(*) AS n_months,
           avg(merge_ratio) AS avg_merge_ratio,
           avg(active_contributors) AS avg_authors
    FROM months
    GROUP BY ALL
    ORDER BY repo, load_bin
"""
LOAD_BIN_LABELS = ["Quiet months\n(< 0.8x the repo's average)", "Normal months\n(0.8x - 1.2x)", "Busy months\n(> 1.2x)"]

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
ARCHIVE_DIR = PROJECT_DIR / "data" / "archive"
LOG_DIR = PROJECT_DIR / "logs"
PLOT_DIR = PROJECT_DIR / "plots"
LOADED_MARKER = RAW_DIR / "_LOADED"
DB_PATH = PROJECT_DIR / "packages.duckdb"
DBT_DIR = PROJECT_DIR  # dbt_project.yml and profiles.yml sit next to main.py

# Prefect already puts a handler on the root logger, so attach a file handler to each logger directly.
# Add a name here for each new task (dbt, validate, model, plot).
LOG_DIR.mkdir(exist_ok=True)
for name in ["extract", "load", "transform", "validate", "model", "plot"]:
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_DIR / f"{name}.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)

logger = logging.getLogger("extract")
load_logger = logging.getLogger("load")
transform_logger = logging.getLogger("transform")
validate_logger = logging.getLogger("validate")
model_logger = logging.getLogger("model")
plot_logger = logging.getLogger("plot")


# Token: local runs use the GH_PAT env var; a deployment run on the server has no env var,
# so it loads the Prefect Secret block instead (DEPLOYMENTS.md "Blocks / Secrets")
TOKEN = os.environ.get("GH_PAT") or Secret.load("rhk2dx-pat").get()
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": API_VERSION,
}


class RunInterrupted(Exception):
    """Raised in worker threads after Ctrl-C; never retried."""


def pause(seconds: float) -> None:
    """Sleep, but wake early and raise if the run is interrupted."""
    if STOP.wait(seconds):
        raise RunInterrupted("Stopped by Ctrl-C")


def github_get(url: str, params: dict | None = None) -> httpx.Response:
    """GET one URL and check quota. Raises httpx.HTTPError on a failed request so Prefect retries the task."""
    with RATE_LOCK:
        pass  # if another thread is sleeping off the rate limit, wait here until it's done
    if STOP.is_set():
        raise RunInterrupted("Stopped by Ctrl-C")
    response = httpx.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    global API_REQUESTS
    with ODOMETER_LOCK:  # three threads add to it at once
        API_REQUESTS += 1

    # primary limit: once remaining <= buffer, sleep until the reset time (+5s margin)
    remaining = int(response.headers.get("X-RateLimit-Remaining", RATE_LIMIT_BUFFER + 1))
    reset = int(response.headers.get("X-RateLimit-Reset", time.time()))
    if remaining <= RATE_LIMIT_BUFFER:
        with RATE_LOCK:  # holding the lock pauses every other thread too
            wait = max(reset - time.time(), 0) + 5
            logger.warning(f"Rate limit: {remaining} left; sleeping {wait:.0f}s until reset")
            pause(wait)

    # secondary limit: GitHub says how many seconds to wait before retrying
    if response.status_code in (403, 429) and "Retry-After" in response.headers:
        wait = int(response.headers["Retry-After"])
        logger.warning(f"Secondary rate limit {response.status_code}; sleeping {wait}s")
        with RATE_LOCK:
            pause(wait)

    response.raise_for_status()  # any 4xx/5xx raises httpx.HTTPStatusError
    return response


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
        try:
            response = github_get(url, params)
            data = response.json()
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"[{repo}] {endpoint} page {page} failed: {e}")
            raise  # re-raise so Prefect's retries kick in
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
    """README 'Resume or start fresh?': _LOADED present means data/raw is in DuckDB.
    Move it to data/archive/raw_<UTC time>/ (instead of deleting it) and start fresh."""
    if LOADED_MARKER.exists():
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        RAW_DIR.rename(ARCHIVE_DIR / f"raw_{stamp}")
        logger.info(f"Moved loaded files to {ARCHIVE_DIR / f'raw_{stamp}'}")
        return "fresh"
    return "resume"


def read_watermarks() -> dict:
    """{(repo, endpoint): newest updated_at loaded} from _load_state; empty before the first load."""
    if not DB_PATH.exists():
        return {}
    try:
        with duckdb.connect(DB_PATH) as con:
            rows = con.execute("SELECT repo, endpoint, max_updated_at FROM _load_state "
                               "WHERE endpoint IN ('commits', 'pulls')").fetchall()
    except duckdb.Error:
        return {}  # database exists but no load has finished yet
    watermarks = {}
    for repo, endpoint, ts in rows:
        watermarks[(repo, endpoint)] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    return watermarks


def retry_if_transient(task, task_run, state) -> bool:
    """Prefect retry condition: retry only failures that can succeed on a later try.

    Not retried: a Ctrl-C stop, and 4xx errors other than 403/429 (a 404 or 401 fails the same way every time).
    Retried: 403/429 (rate limits), 5xx (GitHub server errors), timeouts and connection errors, broken JSON.
    """
    try:
        state.result()
    except RunInterrupted:
        return False
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        return status >= 500 or status in (403, 429)
    except Exception:
        return True
    return True


@task(name="fetch-repo", task_run_name="{repo}", retries=3, retry_delay_seconds=[10, 30, 60],
      retry_condition_fn=retry_if_transient)
def fetch_repo(repo: str, commits_since: str, pulls_since: str, window_start: str) -> int:
    """All four endpoints for one repo: pulls, then their reviews, then commits and releases. Returns requests made."""
    logger.info(f"[{repo}] commits since {commits_since}, pulls updated since {pulls_since}")
    prs, n_pulls = fetch_pages(repo, "pulls", PULLS_PARAMS, stop_before=pulls_since)
    # review calls only for PRs updated since the watermark (README Task 2, incremental loads)
    updated = [pr for pr in prs if pr["updated_at"] >= pulls_since]
    reviews, n_reviews = fetch_reviews(repo, updated, created_since=window_start)
    commits, n_commits = fetch_pages(repo, "commits", {"since": commits_since})
    releases, n_releases = fetch_pages(repo, "releases", {})
    requests = n_pulls + n_reviews + n_commits + n_releases
    logger.info(f"[{repo}] pulls {len(prs)}, reviews {len(reviews)}, commits {len(commits)}, "
                f"releases {len(releases)}; {requests} requests")
    return requests


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

    # one row per record: unnest each file's "data" list, then pull out the key and updated_at
    con.execute(f"""
        INSERT OR REPLACE INTO {table}
        SELECT repo, {spec['id']} AS id, {spec['updated_at']} AS updated_at, fetched_at,
               filename AS source_file, rec AS payload
        FROM (SELECT repo, fetched_at, filename, unnest(data) AS rec
              FROM read_json('{RAW_DIR}/{spec['files']}',
                             columns = {{repo: 'VARCHAR', fetched_at: 'TIMESTAMP', data: 'JSON[]'}},
                             filename = true))
    """)
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


@task(name="load-table", task_run_name="load {endpoint}")
def load_table_task(endpoint: str) -> int:
    with duckdb.connect(DB_PATH) as con:
        return load_table(con, endpoint)


@task(name="finish-load")
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
    mode = prepare_raw_dir()
    watermarks = read_watermarks()
    logger.info(f"Extract starting ({mode}): {len(REPOS)} repos, window from {window_start}")
    # incremental: fetch commits and pulls only since the newest ones already loaded (README Task 2)
    commits_since = [watermarks.get((r, "commits"), window_start) for r in REPOS]
    pulls_since = [watermarks.get((r, "pulls"), window_start) for r in REPOS]
    try:
        # one task per repo, run in parallel; .result() raises if any repo failed after its retries
        counts = fetch_repo.map(REPOS, commits_since, pulls_since, unmapped(window_start)).result()
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
    load_logger.info(f"Load starting: {RAW_DIR} -> {DB_PATH}")
    for endpoint in LOAD_SPECS:  # one at a time: DuckDB allows a single writer
        load_table_task(endpoint)
    finish_load_task()


@task(name="dbt-build", log_prints=True)
def dbt_build():
    """Task 3: install dbt packages, then build and test every model. Fails if any model or test fails."""
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=str(DBT_DIR),
            profiles_dir=str(DBT_DIR),
        )
    )
    transform_logger.info("Running dbt deps")
    runner.invoke(["deps"])
    transform_logger.info("Running dbt build")
    result = runner.invoke(["build"])
    transform_logger.info("dbt build finished: %d models and tests passed", len(result.result))
    return result


def landed_globs(spec: dict) -> list[str]:
    """File patterns for one endpoint in data/raw/ and every archived raw_* folder, skipping ones with no files."""
    folders = [RAW_DIR] + sorted(ARCHIVE_DIR.glob("raw_*"))
    return [str(folder / spec["files"]) for folder in folders if any(folder.glob(spec["files"]))]


def check_landed_vs_raw(con: duckdb.DuckDBPyConnection) -> list[str]:
    """A1: every distinct (repo, id) in the landed files is in the raw table. Returns failure messages."""
    failures = []
    for endpoint, spec in LOAD_SPECS.items():
        rows = con.execute(f"""
            WITH landed AS (
                SELECT DISTINCT repo, ({spec['id']})::{spec['id_type']} AS id
                FROM (SELECT repo, unnest(data) AS rec
                      FROM read_json({landed_globs(spec)},
                                     columns = {{repo: 'VARCHAR', fetched_at: 'TIMESTAMP', data: 'JSON[]'}}))
            )
            SELECT landed.repo, count(*) AS landed, sum(CASE WHEN t.id IS NULL THEN 1 ELSE 0 END) AS missing
            FROM landed
            LEFT JOIN {spec['table']} t ON t.repo = landed.repo AND t.id = landed.id
            GROUP BY ALL ORDER BY landed.repo
        """).fetchall()
        for repo, landed, missing in rows:
            validate_logger.info(f"[{repo}] {endpoint}: {landed} distinct landed records, {missing} missing from {spec['table']}")
            if missing:
                failures.append(f"{repo} {endpoint}: {missing} landed records missing from {spec['table']}")
    return failures


def check_raw_vs_staging(con: duckdb.DuckDBPyConnection) -> list[str]:
    """A2: raw row count minus intentionally dropped rows equals the staging row count, per repo and endpoint."""
    failures = []
    for endpoint, spec in LOAD_SPECS.items():
        stg = STAGING_SPECS[endpoint]
        rows = con.execute(f"""
            WITH raw AS (
                SELECT repo, count(*) AS raw_rows, sum(CASE WHEN {stg['dropped']} THEN 1 ELSE 0 END) AS dropped
                FROM {spec['table']} GROUP BY ALL
            ),
            staged AS (
                SELECT repo, count(*) AS stg_rows FROM {stg['model']} GROUP BY ALL
            )
            SELECT raw.repo, raw.raw_rows, raw.dropped, coalesce(staged.stg_rows, 0)
            FROM raw LEFT JOIN staged USING (repo) ORDER BY raw.repo
        """).fetchall()
        for repo, raw_rows, dropped, stg_rows in rows:
            validate_logger.info(f"[{repo}] {endpoint}: {raw_rows} raw - {dropped} dropped = {raw_rows - dropped}, {stg['model']} has {stg_rows}")
            if raw_rows - dropped != stg_rows:
                failures.append(f"{repo} {endpoint}: {raw_rows - dropped} expected in {stg['model']}, found {stg_rows}")
    return failures


def check_monthly_commits(con: duckdb.DuckDBPyConnection) -> list[str]:
    """A3: every repo has commits in every complete month.

    Caveat to the README rule: a month with 0 commits passes (with a warning) only if the secondary sanity
    check also finds 0 merged PRs that month. Every merge adds a commit to the default branch, so
    "no merges and no commits" means the repo was inactive, while "merges but no commits" means lost data.
    """
    failures = []
    rows = con.execute("""
        SELECT repo, strftime(month, '%Y-%m'), prs_merged, prs_opened
        FROM mart_repo_monthly
        WHERE NOT is_partial_month AND commits = 0
        ORDER BY repo, month
    """).fetchall()
    for repo, month, merged, opened in rows:
        if merged == 0:
            validate_logger.warning(
                f"[{repo}] {month}: 0 commits. Passed by the secondary sanity check (0 merged PRs, "
                f"{opened} opened): the repo was inactive that month, not missing data."
            )
        else:
            failures.append(f"{repo} {month}: 0 commits but {merged} merged PRs, so commits are missing")
    validate_logger.info(f"Monthly commit check: {len(rows)} zero-commit complete months, {len(failures)} failures")
    return failures


def log_outliers(con: duckdb.DuckDBPyConnection) -> None:
    """B: flag slow merges and commit bursts, list them, and keep them all (see README write-up)."""
    # Separate cutoffs per author category: a week is normal for a human PR but means an automated PR got stuck.
    slow = con.execute(f"""
        WITH cutoffs AS (
            SELECT repo, quantile_cont(hours_to_merge, {HUMAN_MERGE_QUANTILE}) AS human_cutoff
            FROM fct_pull_requests WHERE is_merged AND NOT author_is_bot GROUP BY ALL
        )
        SELECT f.repo, f.number, f.author_login, f.author_is_bot,
               round(f.hours_to_merge / 24, 1) AS days,
               round(CASE WHEN f.author_is_bot THEN {BOT_MERGE_HOURS} ELSE c.human_cutoff END / 24, 1) AS cutoff_days
        FROM fct_pull_requests f
        JOIN cutoffs c USING (repo)
        WHERE f.is_merged
            AND f.hours_to_merge > CASE WHEN f.author_is_bot THEN {BOT_MERGE_HOURS} ELSE c.human_cutoff END
        ORDER BY f.repo, f.author_is_bot DESC, f.hours_to_merge DESC
    """).fetchall()
    validate_logger.info(
        f"Merge-time outliers: human PRs above their repo's {HUMAN_MERGE_QUANTILE:.0%} quantile, "
        f"bot PRs above {BOT_MERGE_HOURS / 24:.0f} days. {len(slow)} flagged, all kept (real merges; medians are robust)."
    )
    for repo, number, author, is_bot, days, cutoff_days in slow:
        kind = "bot" if is_bot else "human"
        validate_logger.info(f"  [{repo}] PR #{number} by {author} ({kind}): {days} days to merge (cutoff {cutoff_days})")

    bursts = con.execute(f"""
        WITH weeks AS (
            SELECT repo, date_trunc('week', committed_at)::date AS week, count(*) AS commits,
                   sum(is_bot::int) AS bot_commits
            FROM stg_commits WHERE committed_at >= '{WINDOW_START[:10]}' GROUP BY ALL
        ),
        cutoffs AS (
            SELECT repo, quantile_cont(commits, 0.75)
                       + {COMMIT_BURST_IQR} * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) AS cutoff
            FROM weeks GROUP BY ALL
        )
        SELECT weeks.repo, weeks.week, weeks.commits, weeks.bot_commits, round(cutoffs.cutoff)
        FROM weeks JOIN cutoffs USING (repo)
        WHERE weeks.commits > cutoffs.cutoff
        ORDER BY weeks.repo, weeks.week
    """).fetchall()
    validate_logger.info(
        f"Weekly commit outliers: weeks above Q3 + {COMMIT_BURST_IQR} x IQR per repo. "
        f"{len(bursts)} flagged, all kept (bot commits counted to spot automated bursts)."
    )
    for repo, week, commits, bot_commits, cutoff in bursts:
        validate_logger.info(f"  [{repo}] week of {week}: {commits} commits, {bot_commits} by bots (cutoff {cutoff:.0f})")


def log_descriptive_stats(con: duckdb.DuckDBPyConnection) -> None:
    """C: per-repo medians and 90th percentiles from the marts, commits per week, releases per year."""
    rows = con.execute(f"""
        WITH prs AS (
            SELECT repo,
                   median(hours_to_first_review) AS review_p50,
                   quantile_cont(hours_to_first_review, 0.9) AS review_p90,
                   median(hours_to_merge) AS merge_p50,
                   quantile_cont(hours_to_merge, 0.9) AS merge_p90
            FROM fct_pull_requests GROUP BY ALL
        ),
        commits AS (
            -- complete months only: the days from the window start to the first day of the partial month
            SELECT repo,
                   sum(CASE WHEN NOT is_partial_month THEN commits ELSE 0 END)
                       / (date_diff('day', DATE '{WINDOW_START[:10]}', max(month)) / 7.0) AS commits_per_week
            FROM mart_repo_monthly GROUP BY ALL
        ),
        releases AS (
            -- full releases only (prereleases excluded), over the days from the window start to run time
            SELECT repo,
                   sum(CASE WHEN NOT is_prerelease THEN 1 ELSE 0 END)
                       / (date_diff('day', DATE '{WINDOW_START[:10]}',
                                    (SELECT max(fetched_at) FROM raw_pull_requests)) / 365.25) AS releases_per_year
            FROM fct_releases GROUP BY ALL
        )
        SELECT repo, review_p50, review_p90, merge_p50, merge_p90, commits_per_week, releases_per_year
        FROM prs JOIN commits USING (repo) JOIN releases USING (repo)
        ORDER BY repo
    """).fetchall()
    validate_logger.info("Descriptive statistics (hours; medians and 90th percentiles skip unreviewed / unmerged PRs):")
    validate_logger.info(f"  {'repo':<27}{'review p50':>11}{'review p90':>11}{'merge p50':>10}{'merge p90':>10}{'commits/wk':>11}{'releases/yr':>12}")
    for repo, r50, r90, m50, m90, cpw, rpy in rows:
        validate_logger.info(f"  {repo:<27}{r50:>11.1f}{r90:>11.1f}{m50:>10.1f}{m90:>10.1f}{cpw:>11.1f}{rpy:>12.1f}")


@task(name="validate")
def validate() -> None:
    """Task 4: end-to-end checks from landed files to raw tables to staging, then outliers and statistics.

    Runs every check before failing, so one run lists every problem. Raises if any completeness check fails.
    """
    # no read_only=True: dbt runs in this same process and keeps a read-write connection open,
    # and DuckDB refuses a second connection to the same file with different settings
    with duckdb.connect(DB_PATH) as con:
        con.execute("SET TimeZone = 'UTC'")
        failures = check_landed_vs_raw(con) + check_raw_vs_staging(con) + check_monthly_commits(con)
        log_outliers(con)
        log_descriptive_stats(con)
    if failures:
        raise ValueError("Validation failed:\n" + "\n".join(failures))
    validate_logger.info("All completeness checks passed")


@task(name="score-health")
def score_health() -> None:
    """Task 5: score each repo per dimension (0-1), combine with SCORE_SPECS weights, rank, save to health_scores."""
    dimensions = []
    subscores = []
    weighted = []
    for metric, spec in SCORE_SPECS.items():
        dimension = spec["dimension"]
        dimensions.append(dimension)
        subscores.append(
            f"greatest(0, least(1, ({metric} - {spec['zero_at']}) / ({spec['one_at']} - {spec['zero_at']}))) AS {dimension}"
        )
        weighted.append(f"{spec['weight']} * {dimension}")

    # no read_only=True: dbt keeps a read-write connection open in this process (see validate)
    with duckdb.connect(DB_PATH) as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE health_scores AS
            WITH subscores AS (
                SELECT repo, {", ".join(subscores)}
                FROM mart_health_inputs
            )
            SELECT rank() OVER (ORDER BY {" + ".join(weighted)} DESC) AS rank,
                   repo,
                   ({" + ".join(weighted)}) / 100 AS score,
                   {", ".join(dimensions)}
            FROM subscores
            ORDER BY rank
        """)
        rows = con.execute("SELECT * FROM health_scores ORDER BY rank").fetchall()
        # robustness check: same subscores, equal weights; logged only
        equal = con.execute(f"""
            SELECT rank() OVER (ORDER BY ({" + ".join(dimensions)}) DESC) AS rank,
                   repo, ({" + ".join(dimensions)}) / {len(dimensions)} AS score
            FROM health_scores
            ORDER BY rank
        """).fetchall()

    model_logger.info("Health ranking (score and subscores 0-1, 1 = healthiest; weights: "
                      + ", ".join(f"{s['dimension']} {s['weight']}" for s in SCORE_SPECS.values()) + "):")
    model_logger.info(f"  {'rank':<5}{'repo':<27}{'score':>6}" + "".join(f"{d:>16}" for d in dimensions))
    for row in rows:
        rank, repo, score = row[:3]
        model_logger.info(f"  {rank:<5}{repo:<27}{score:>6.3f}" + "".join(f"{s:>16.3f}" for s in row[3:]))
    model_logger.info("Robustness check, equal weights (1/6 each, not stored):")
    for rank, repo, score in equal:
        model_logger.info(f"  {rank:<5}{repo:<27}{score:>6.3f}")


def add_titles(fig, title: str, subtitle: str) -> None:
    """Bold left-aligned title with a gray subtitle under it (images/commit_activity.png), then fit the panels below."""
    height = fig.get_figheight()  # inches, so the title area is the same size on every figure
    fig.text(0.01, 1 - 0.25 / height, title, ha="left", va="top", fontweight="bold", fontsize=18, color="#222222")
    fig.text(0.01, 1 - 0.7 / height, subtitle, ha="left", va="top", fontsize=11, color="#777777")
    fig.tight_layout(rect=(0, 0, 1, 1 - 1.15 / height))


def plot_dimension_profile(con: duckdb.DuckDBPyConnection) -> Path:
    """Plot 1: every scored dimension for all three repos, bars in rank order."""
    dimensions = [spec["dimension"] for spec in SCORE_SPECS.values()]
    labels = [f"{spec['dimension'].replace('_', ' ').capitalize()} ({spec['weight']}%)" for spec in SCORE_SPECS.values()]
    rows = con.execute(f"SELECT rank, repo, score, {', '.join(dimensions)} FROM health_scores ORDER BY rank").fetchall()

    # bars run left to right, so the grid lines are vertical here
    with plt.rc_context(EXAMPLE_STYLE | {"axes.grid.axis": "x"}):
        fig, ax = plt.subplots(figsize=(12, 8))
        bar_height = 0.8 / len(rows)
        for i, row in enumerate(rows):
            rank, repo, score = row[:3]
            # one bar per dimension, shifted down by i so the 3 repos sit side by side in rank order
            positions = [d + i * bar_height for d in range(len(dimensions))]
            bars = ax.barh(positions, row[3:], height=bar_height, color=REPO_COLORS[repo],
                           label=f"#{rank} {repo} · score {score:.3f}")
            ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=9, color="#777777")
        ax.set_yticks([d + bar_height for d in range(len(dimensions))], labels, color="#222222", fontsize=11)
        ax.invert_yaxis()  # highest-weight dimension at the top, #1 repo first in each group
        ax.set_xlim(0, 1)
        ax.set_axisbelow(True)  # grid lines behind the bars, not across them
        ax.set_xlabel("Subscore (0 = least healthy, 1 = healthiest)")
        ax.set_ylabel("Dimension (weight in total score)")
        fig.legend(loc="upper right", bbox_to_anchor=(0.99, 0.99), fontsize=11)  # top right, in rank order
        add_titles(fig, "Health subscores by dimension",
                   "bars in rank order within each dimension · weight in parentheses")
        path = PLOT_DIR / "dimension_profile.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
    return path


def plot_monthly_trends(con: duckdb.DuckDBPyConnection, repos: list[str]) -> Path:
    """Plot 2: one row per monthly metric, one column per repo, the column's repo highlighted."""
    # 3-month rolling mean per repo and metric (current and 2 previous months), like images/commit_activity.png
    rolling = ", ".join(
        f"avg({column}) OVER (PARTITION BY repo ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS {column}_rolling"
        for column, _, _, _, _ in TREND_PANELS
    )
    df = con.execute(f"SELECT *, {rolling} FROM mart_repo_monthly WHERE NOT is_partial_month ORDER BY repo, month").df()

    with plt.rc_context(EXAMPLE_STYLE):
        fig, axes = plt.subplots(len(TREND_PANELS), len(repos), figsize=(16, 26), sharex=True, sharey="row")
        for r, (column, title, unit, summary, tier) in enumerate(TREND_PANELS):
            for c, repo in enumerate(repos):
                ax = axes[r, c]
                for other in repos:  # the other two repos in faded gray, for comparison
                    if other != repo:
                        other_df = df[df["repo"] == other]
                        ax.plot(other_df["month"], other_df[f"{column}_rolling"], color="#cccccc", linewidth=1)
                repo_df = df[df["repo"] == repo]
                ax.scatter(repo_df["month"], repo_df[column], color=REPO_COLORS[repo], alpha=0.35, s=12)
                ax.plot(repo_df["month"], repo_df[f"{column}_rolling"], color=REPO_COLORS[repo], linewidth=2.5)
                if summary == "total":
                    note = f"{tier} · {title} · {repo_df[column].sum():,.0f} total"
                else:
                    note = f"{tier} · {title} · median {repo_df[column].median():,.2f}"
                ax.set_title(f"#{c + 1} {repo}\n", loc="left", fontweight="bold", fontsize=12)
                ax.text(0, 1.02, note, transform=ax.transAxes, color="#777777", fontsize=10)
                if column == "median_first_review_hours":
                    ax.set_yscale("log")  # median review time is heavily right-skewed
                ax.set_ylabel(unit)
                ax.set_xlabel("Month")
                ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))  # Jan and Jul ticks, like the example
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
                # sharex / sharey hide inner tick labels; show them on every panel so each reads on its own
                ax.tick_params(labelbottom=True, labelleft=True)
        add_titles(fig, f"Monthly metrics per repo ({df['month'].min():%b %Y} - {df['month'].max():%b %Y})",
                   "line = 3-month rolling mean · dots = raw monthly values · gray = the other two repos · "
                   "columns in rank order · rows in Task 5 weight-tier order\n"
                   "authors exclude bots · current partial month excluded")
        path = PLOT_DIR / "monthly_trends.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
    return path


def plot_load_vs_rejection(con: duckdb.DuckDBPyConnection, repos: list[str]) -> Path:
    """Plot 3: average merge ratio in quiet, normal and busy months; bubble size = average active PR authors."""
    df = con.execute(LOAD_BINS_SQL).df()

    with plt.rc_context(EXAMPLE_STYLE):
        fig, ax = plt.subplots(figsize=(12, 8))
        for i, repo in enumerate(repos):
            repo_df = df[df["repo"] == repo]
            x = repo_df["load_bin"] + (i - 1) * 0.08  # small sideways shift so bubbles in the same bin do not overlap
            ax.plot(x, repo_df["avg_merge_ratio"], color=REPO_COLORS[repo], linewidth=2, zorder=1)
            ax.scatter(x, repo_df["avg_merge_ratio"], s=repo_df["avg_authors"] * 12, color=REPO_COLORS[repo],
                       alpha=0.8, edgecolor="white", zorder=2)
            for xi, y, authors, n in zip(x, repo_df["avg_merge_ratio"], repo_df["avg_authors"], repo_df["n_months"]):
                # first repo's labels go left of its bubbles, the others right, so labels in the same bin never overlap
                ax.annotate(f"{y:.2f}\n{authors:.0f} authors, {n} mo", (xi, y), textcoords="offset points",
                            xytext=(-14, -4) if i == 0 else (14, -4), ha="right" if i == 0 else "left",
                            fontsize=9, color=REPO_COLORS[repo],
                            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.85, "pad": 1})  # readable over lines
        ax.set_xticks([0, 1, 2], LOAD_BIN_LABELS, color="#222222", fontsize=11)
        ax.set_xlim(-0.55, 2.6)
        # merge ratios here run 0.62-0.84; 0.5-0.9 leaves room for labels without exaggerating the drops
        ax.set_ylim(0.5, 0.9)
        ax.set_xlabel("PRs opened that month, relative to the repo's own monthly average")
        ax.set_ylabel("Average merge ratio (merged / (merged + closed unmerged))")
        # same-size legend markers (the bubbles' own sizes would make the legend uneven), top right in rank order
        handles = [plt.Line2D([], [], marker="o", linestyle="-", color=REPO_COLORS[repo], markersize=8) for repo in repos]
        fig.legend(handles, [f"#{i + 1} {repo}" for i, repo in enumerate(repos)],
                   loc="upper right", bbox_to_anchor=(0.99, 0.99), fontsize=11)
        add_titles(fig, "Load vs rejection",
                   "do busier months merge a smaller share of PRs? · bubble size = average active PR authors (non-bot) "
                   "in those months\nlabels = average merge ratio, authors, number of months · current partial month excluded")
        path = PLOT_DIR / "load_vs_rejection.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
    return path


@task(name="plot-health")
def plot_health() -> None:
    """Task 6: save the three plots as PNG to plots/."""
    PLOT_DIR.mkdir(exist_ok=True)
    # no read_only=True: dbt keeps a read-write connection open in this process (see validate)
    with duckdb.connect(DB_PATH) as con:
        repos = [row[0] for row in con.execute("SELECT repo FROM health_scores ORDER BY rank").fetchall()]  # rank order
        paths = [plot_dimension_profile(con), plot_monthly_trends(con, repos), plot_load_vs_rejection(con, repos)]
    for path in paths:
        plot_logger.info(f"Saved {path}")


@flow(name="rhk2dx-package-health-flow")
def package_health_flow() -> None:
    global API_REQUESTS
    API_REQUESTS = 0
    try:
        extract()
        load()
        dbt_build()
        validate()
        score_health()
        plot_health()
    finally:
        logger.info(f"API odometer: {API_REQUESTS} GitHub API requests this flow run")


def deploy() -> None:
    """Publish the flow as a deployment that runs midnight Eastern on the 28th of every month (DEPLOYMENTS.md)."""
    package_health_flow.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            # private repo: the worker clones with the PAT. Pass the block itself, not .get(),
            # so only a reference to the block is stored in the deployment
            credentials={"access_token": Secret.load("rhk2dx-pat")},
        ),
        entrypoint="main.py:package_health_flow",
    ).deploy(
        name="rhk2dx-deployment",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )


if __name__ == "__main__":
    if "--deploy" in sys.argv:
        deploy()               # python main.py --deploy : publish the monthly deployment
    else:
        package_health_flow()  # python main.py          : run the pipeline
