# Open Source Package Health: GitHub Data Pipeline

DS3022 Data Project 2 · rhk2dx · Package set B. The assignment text is in [PROJECT_INSTRUCTIONS.md](PROJECT_INSTRUCTIONS.md).

## 1. Overview

One Prefect flow pulls GitHub activity for three Python data science packages (package set B) and ranks their development communities from healthiest to least healthy.

| Repo                        | PRs created since 2024-01-01 | Commits since 2024-01-01 |
|-----------------------------|------------------------------|--------------------------|
| `scikit-learn/scikit-learn` | 5,228                        | 3,364                    |
| `dask/dask`                 | 1,231                        | 1,371                    |
| `plotly/plotly.py`          | 690                          | 2,829                    |

Analysis window: 2024-01-01 through the time of each run (35 months, January 2024 to November 2026, as of the last run).

### The flow

`rhk2dx-package-health-flow` in `main.py` runs six tasks in order:

| #   | Task                | What it does                                                                         | Output                                               |
|-----|---------------------|--------------------------------------------------------------------------------------|------------------------------------------------------|
| 1   | `extract` (subflow) | Fetches commits, pull requests, PR reviews, and releases for the 3 repos in parallel | `data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json` |
| 2   | `load` (subflow)    | Upserts every landed record into one raw table per endpoint                          | `raw_*` tables, `_load_state`, `data/raw/_LOADED`    |
| 3   | `dbt_build`         | Runs `dbt build`: staging models, fact and mart tables, 43 tests                     | `stg_*`, `fct_*`, `mart_*`                           |
| 4   | `validate`          | Python checks from landed files to staging; outliers; descriptive statistics         | log only (fails the flow on a mismatch)              |
| 5   | `score_health`      | Scores 6 health dimensions from `mart_health_inputs`, weights them, ranks the repos  | `health_scores` table                                |
| 6   | `plot_health`       | Draws 3 plots                                                                        | `plots/*.png`                                        |

### Result

| Rank | Repo                        | Health score (0–1) |
|------|-----------------------------|--------------------|
| 1    | `plotly/plotly.py`          | 0.676              |
| 2    | `scikit-learn/scikit-learn` | 0.615              |
| 3    | `dask/dask`                 | 0.550              |

With equal weights the order is the same (0.666 / 0.603 / 0.517). Section 9 explains the metrics and weights. Section 11 covers dask's rank: its merging slowed sharply from July 2026 (1 merge each in August and September, against an average of 30.5 per month in 2024–25), and it has not merged a PR since 2026-09-29, though maintainers still review and close PRs.

## 2. Setup and run

### Requirements

- Python 3.12
- A GitHub personal access token. A classic token with no scopes is enough, because all three repos are public.
- All Python dependencies are pinned in `requirements.txt` (dbt-core 1.12.6, dbt-duckdb 1.11.1, duckdb 2.0.0, httpx 0.28.1, prefect 3.9.0, prefect-dbt 0.7.25).

### Setup

```bash
git clone https://github.com/MaxGuryan9/package-health-project-ds3022.git
cd package-health-project-ds3022
uv venv proj2 --python 3.12         # or: python3.12 -m venv proj2
source proj2/bin/activate
uv pip install -r requirements.txt  # or: pip install -r requirements.txt
export GH_PAT=<your token>
```

The token is read only from the `GH_PAT` environment variable (`main.py`). It is never written to a file, and `.gitignore` excludes `.env` files.

Nothing else needs to be set up: the flow runs `dbt deps` itself, and it creates `data/`, `logs/`, and `packages.duckdb` on the first run.

### Run

```bash
python main.py
```

This runs one Prefect flow, `rhk2dx-package-health-flow`: extract → load → dbt build → validate → score → plot.

### Expected runtime

| Run                                      | API requests | Time                                                                           | Source                                                                                                |
|------------------------------------------|--------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| First run (full window, nothing on disk) | 7,034        | Extract about 35 min; load, dbt, validation, scoring, and plots add under 15 s | `logs/extract_full_run.log` (2026-11-01, 17:59 → 18:35, including a 3-minute deliberate interruption) |
| Later runs (incremental)                 | 13–61        | About 20 s for the whole flow                                                  | `logs/extract.log` → `logs/plot.log` (2026-11-04: 33 requests, 19 s)                                  |

A first run needs more requests than the 5,000-per-hour limit. In the measured run, GitHub's hourly reset came partway through, so the flow never had to sleep. A run that starts with a full 5,000 budget will use it in about 24 minutes, then sleep until the reset. That makes a first run take up to about 70 minutes.

**Tip: start a full run shortly before your rate limit resets.** GitHub's limit is not reset on the clock hour. It resets one hour after the first request of the current window. A full run that starts shortly before a reset gets a fresh 5,000 requests early, so it has little or no sleep. That is what happened in the measured run: the budget reset about 10 minutes after the start, at 18:08:46 (`logs/extract_full_run.log`), so the run never slept. To check your remaining requests and reset time (this call does not count against the limit):

```bash
curl -s -H "Authorization: Bearer $GH_PAT" https://api.github.com/rate_limit | grep -A3 '"core"'
```

`reset` is a Unix timestamp; `date -r <reset>` (macOS) shows it as a local time.

### Outputs

- `packages.duckdb`: raw tables, dbt models, and the `health_scores` ranking
- `plots/*.png`: three plots
- `logs/`: one log per task (`extract`, `load`, `transform`, `validate`, `model`, `plot`), plus dbt's own `dbt.log`

## 3. Repository layout

```
package-health-project-ds3022/
├── main.py                        # the Prefect flow and all six tasks
├── requirements.txt               # pinned dependencies
│
├── dbt_project.yml                # dbt project: staging = views, marts = tables
├── profiles.yml                   # dbt connection: packages.duckdb, UTC (no secrets)
├── packages.yml                   # dbt package dbt_utils (installed by `dbt deps` in the flow)
├── package-lock.yml               # pinned dbt package version
├── models/
│   ├── staging/
│   │   ├── sources.yml            # raw_* tables declared as dbt sources
│   │   ├── schema.yml             # staging tests
│   │   ├── stg_commits.sql
│   │   ├── stg_pull_requests.sql
│   │   ├── stg_pr_reviews.sql
│   │   └── stg_releases.sql
│   └── marts/
│       ├── schema.yml             # descriptions and tests for every mart and column
│       ├── fct_pull_requests.sql
│       ├── fct_releases.sql
│       ├── mart_repo_monthly.sql
│       └── mart_health_inputs.sql
│
├── tests/
│   ├── assert_pr_closed_after_created.sql   # dbt singular test
│   ├── assert_review_after_pr_created.sql   # dbt singular test
│   └── *_test.py, test_fetch_pages.py       # Python tests (section 13)
│
├── plots/                         # the three PNGs written by Task 6
│   ├── dimension_profile.png
│   ├── monthly_trends.png
│   └── load_vs_rejection.png
│
│   # Template reference files (supplied with the assignment, not our work)
├── PROJECT_INSTRUCTIONS.md        # the assignment text (was the template's README.md)
├── CHECKLIST.md                   # submission checklist
├── ASSESSING-HEALTH.md            # background on measuring open source health
├── DBT-REFERENCE.md               # dbt layout, profiles, and PrefectDbtRunner reference
├── DEPLOYMENTS.md                 # Prefect deployment instructions (extended item)
├── EXTRAS.md                      # instructor tips
│
└── README.md                      # this write-up
```

dbt reads `tests/` for singular tests (`test-paths` in `dbt_project.yml`), so the dbt tests and the Python tests share that folder.

**Not committed** (in `.gitignore`), because each is created by running the flow, holds local state, or is not project output:

| Excluded                                | What it is                                                                                    |
|-----------------------------------------|-----------------------------------------------------------------------------------------------|
| `data/`                                 | Landed API pages (`data/raw/`) and earlier runs' pages (`data/archive/`)                      |
| `packages.duckdb`                       | The database                                                                                  |
| `logs/`                                 | Task logs, `dbt.log`, and test logs (`logs/test_logs/`)                                       |
| `target/`, `dbt_packages/`              | dbt build output and installed dbt packages                                                   |
| `proj2/`                                | The virtual environment                                                                       |
| `images/`                               | Example plots from the assignment template; kept locally as the style reference for our plots |
| `.env`, `__pycache__/`, `tests/output/` | Secrets, Python caches, test scratch files                                                    |

## 4. Pipeline-wide design

### Retries and error handling

Only Task 1 talks to the network, so it is the only task with retries.

| Failure                                                  | Where                                   | What happens                                                                                                                                                                                                                                                                                                                                                       |
|----------------------------------------------------------|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Any failed request (4xx, 5xx, timeout, connection error) | `github_get`                            | `response.raise_for_status()` raises `httpx.HTTPError`                                                                                                                                                                                                                                                                                                             |
| Broken JSON in a response                                | `fetch_pages`                           | `response.json()` raises `ValueError`                                                                                                                                                                                                                                                                                                                              |
| Either of the above                                      | `fetch_pages`                           | Caught, logged with context (`[repo] endpoint page N failed: <error>`), then re-raised                                                                                                                                                                                                                                                                             |
| Secondary rate limit (403/429 with `Retry-After`)        | `github_get`                            | Sleeps the number of seconds GitHub asks for, then raises, so the retry happens after the wait                                                                                                                                                                                                                                                                     |
| The re-raised error                                      | `fetch_repo` task, `retry_if_transient` | Transient failures (403/429, 5xx, timeouts, connection errors, broken JSON) are retried up to 3 times, with increasing delays of 10, 30, and 60 s. Pages already on disk are skipped, so a retry costs only the requests that had not succeeded. Permanent failures (other 4xx, such as 404 or 401) fail at once, because they would fail the same way every time. |
| Ctrl-C                                                   | `extract`, `retry_if_transient`         | Workers stop at their next request or sleep. A Ctrl-C stop is never retried.                                                                                                                                                                                                                                                                                       |

Tasks 2–6 work on local files and the database. A failure there (a bad file, a failed dbt test, a validation mismatch) would fail again on a retry, so these tasks have no retries and fail the flow immediately.

### Logging

Each task has its own Python logger with a file handler, and the same logger is routed into Prefect (`PREFECT_LOGGING_EXTRA_LOGGERS`), so every line appears in the terminal, in the Prefect UI, and in a file:

| Task         | Log file                             | What the log records (purpose)                                                                                                                |
|--------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| 1 Extract    | `logs/extract.log`                   | Every page fetched with its record count and remaining rate-limit budget; rate-limit sleeps; resume points; total API requests per run        |
| 2 Load       | `logs/load.log`                      | Row count before and after each raw table upsert, which shows loads are idempotent; `_load_state` update and `_LOADED` marker                 |
| 3 dbt        | `logs/transform.log`, `logs/dbt.log` | `transform.log`: start and finish of `dbt deps` and `dbt build`, with the pass count. `dbt.log`: every model built and every test's PASS/FAIL |
| 4 Validation | `logs/validate.log`                  | Completeness checks (landed files → raw → staging, commits every month), flagged outliers, descriptive statistics                             |
| 5 Model      | `logs/model.log`                     | The ranked table (score, rank, 6 subscores) and the equal-weight robustness check                                                             |
| 6 Plot       | `logs/plot.log`                      | The path of each saved PNG                                                                                                                    |

Python tests write their own logs to `logs/test_logs/` (section 13).

## 5. Task 1: Extract

### Endpoints and record counts (first full run, 2026-11-01)

| Endpoint      | Request                                                       | scikit-learn       | dask              | plotly.py       |
|---------------|---------------------------------------------------------------|--------------------|-------------------|-----------------|
| Pull requests | `/pulls?state=all&sort=updated&direction=desc`                | 6,400              | 1,500             | 900             |
| PR reviews    | `/pulls/{number}/reviews`, one call per PR created since 2024 | 18,260 (5,207 PRs) | 1,985 (1,224 PRs) | 1,592 (685 PRs) |
| Commits       | `/commits?since=2024-01-01T00:00:00Z`                         | 3,345              | 1,371             | 2,827           |
| Releases      | `/releases` (full history, small)                             | 51                 | 44                | 109             |

Total: 7,034 API requests. Every count above is logged per repo and endpoint in `logs/extract_full_run.log`.

- **Pull requests** have no date filter, so they are sorted by `updated` descending and paging stops once a page ends before the window start (`stop_before` in `fetch_pages`).
- **Reviews** are fetched for every PR created in the window, merged or not (`fetch_reviews`).
- **Pagination** follows the `Link` header's `rel="next"` URL with `per_page=100` until there is no next page.

### Rate limits

- **Primary limit:** `github_get` reads `X-RateLimit-Remaining` and `X-RateLimit-Reset` on every response. When 25 or fewer requests remain, it sleeps until the reset time plus 5 s. The 25-request buffer leaves room for the other two threads' requests that are already in flight.
- **Secondary limit:** a 403/429 with `Retry-After` sleeps that many seconds, then raises so Prefect retries (section 4).
- 5xx errors and timeouts are retried by Prefect (section 4).

### Concurrent fetching (extended item)

`extract` runs one `fetch_repo` task per repo in parallel (`fetch_repo.map(...)` on a `ThreadPoolTaskRunner` with 3 workers). One token means one shared budget, so all threads share `RATE_LOCK`: when one thread sleeps off the rate limit it holds the lock, and every other thread waits at its next request.

### Landing files

`data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json`, one file per response page, in the README format (`repo`, `endpoint`, `url`, `page`, `fetched_at`, `data`) plus one added field:

- `next_url`: the next page's URL (null on the last page). Resume uses it to continue from the last saved page without re-fetching.

Reviews are stored one folder per PR, `reviews/pr_<number>/page_NNNN.json`, so resume can tell which PRs are done.

Pages are written atomically: first to a `.tmp` file, then renamed (`write_page`). A crash mid-write leaves only a `.tmp` file, which the next run deletes; a half-written `.json` page can never exist.

### Resume after an interruption

When there is no `_LOADED` marker, the next run keeps `data/raw/`, skips every page already on disk, and continues from the saved `next_url`. Excerpt from the first full run, stopped with Ctrl-C after scikit-learn's pulls page 54 and restarted (`logs/extract_full_run.log`, lines 170–190):

```
18:00:01  [scikit-learn/scikit-learn] pulls page 54: 100 records (budget left 4437)
          ← Ctrl-C; restarted 2m 43s later
18:02:44  Extract starting (resume): 3 repos, window from 2024-01-01T00:00:00Z
18:02:45  [plotly/plotly.py] pulls: 9 pages on disk, already complete
18:02:45  [dask/dask] pulls: 15 pages on disk, already complete
18:02:45  [dask/dask] commits: 14 pages on disk, already complete
18:02:45  [scikit-learn/scikit-learn] commits: 34 pages on disk, already complete
...
18:02:45  [scikit-learn/scikit-learn] pulls: 54 pages on disk, resuming at page 55
18:02:47  [scikit-learn/scikit-learn] pulls page 55: 100 records (budget left 4430)
```

The budget went from 4,437 before the stop to 4,430 after the restart, so the pages already on disk cost no requests.

> **Note on the missing Ctrl-C line.** The warning `Ctrl-C received: stopping workers; pages on disk are kept for resume` was added to `extract` after this full run, so it does not appear in the excerpt above. It is confirmed by `tests/interrupt_test.py`, which sends a real Ctrl-C to a running extract (`logs/test_logs/interrupt_test.log`: the warning is logged and the workers stop 0.7 s later). The full run was not repeated to capture the line: the gap was found too close to the deadline, and a fresh full run costs about 7,000 requests and up to 70 minutes.

When `_LOADED` exists, everything on disk is already in DuckDB. The run moves `data/raw/` to `data/archive/raw_<UTC time>/` (kept for Task 4, instead of deleted) and fetches only records newer than the `_load_state` watermarks (section 6).

## 6. Task 2: Load

### Raw tables

One table per endpoint, one row per record (`load_table`):

| Table               | `id` (natural key) | `updated_at` from       |
|---------------------|--------------------|-------------------------|
| `raw_commits`       | `sha`              | `commit.committer.date` |
| `raw_pull_requests` | `number`           | `updated_at`            |
| `raw_pr_reviews`    | `id`               | `submitted_at`          |
| `raw_releases`      | `id`               | `updated_at`            |

Every table has the columns `repo`, `id`, `updated_at`, `fetched_at` (from the landing file), `source_file`, and `payload` (the full record as JSON), with `PRIMARY KEY (repo, id)`. All other fields stay in `payload`; the dbt staging models extract them.

### Idempotent loads

Each table is loaded with `INSERT OR REPLACE` on `(repo, id)`, so a record that is already in the table is replaced, never duplicated. `tests/load_full_test.py` loads the first full run's 7,291 landed pages twice into an empty database (`logs/test_logs/load_full_test.log`):

```
            first load                                     second load
raw_commits: 0 rows before, 7543 after          raw_commits: 7543 rows before, 7543 after
raw_pull_requests: 0 rows before, 8800 after    raw_pull_requests: 8800 rows before, 8800 after
raw_pr_reviews: 0 rows before, 21837 after      raw_pr_reviews: 21837 rows before, 21837 after
raw_releases: 0 rows before, 204 after          raw_releases: 204 rows before, 204 after
```

The test also checks that PR rows equal the distinct PRs in the files (8,800), and that `_load_state` has 12 rows (3 repos × 4 endpoints).

### Load state and incremental runs

After every table has loaded, `finish_load_task` writes `_load_state` (one row per repo and endpoint: newest `updated_at`, row count, load time) and only then creates `data/raw/_LOADED`.

On the next run, `extract` sees `_LOADED`, moves `data/raw/` to `data/archive/raw_<UTC time>/`, and reads the watermarks (`read_watermarks`):

| Endpoint      | Incremental rule                                                                  |
|---------------|-----------------------------------------------------------------------------------|
| Commits       | `since=` the newest loaded commit date                                            |
| Pull requests | Paging stops at the first page that ends before the newest loaded PR `updated_at` |
| Reviews       | Fetched only for PRs updated since the watermark (and created since 2024)         |
| Releases      | Fetched in full (1–2 pages per repo)                                              |

Records fetched again at a watermark boundary are replaced by the upsert, not duplicated.

### Request counts and row counts per run

| Run | Date (local)                                          | Type                                              | API requests | commits | PRs   | reviews | releases |
|-----|-------------------------------------------------------|---------------------------------------------------|--------------|---------|-------|---------|----------|
| 1   | Extract 2026-11-01 17:59–18:35; load 2026-11-02 13:22 | Full window (load task written after the extract) | 7,034        | 7,543   | 8,800 | 21,837  | 204      |
| 2   | 2026-11-02 13:25                                      | Incremental                                       | 61           | 7,549   | 8,811 | 21,871  | 204      |
| 3   | 2026-11-03 22:57                                      | Incremental (with dbt)                            | 60           | 7,563   | 8,828 | 21,913  | 204      |
| 4   | 2026-11-04 00:19                                      | Incremental; failed at validation (see note)      | 15           | 7,563   | 8,828 | 21,914  | 204      |
| 5   | 2026-11-04 00:23                                      | Incremental (with validation)                     | 13           | 7,563   | 8,828 | 21,914  | 204      |
| 6   | 2026-11-04 13:33                                      | Incremental (full flow)                           | 33           | 7,564   | 8,833 | 21,921  | 204      |

Request counts are from `Extract finished: N API requests this run` in `logs/extract.log`; row counts are from `logs/load.log`. An incremental run needs 13–61 requests instead of 7,034.

> Run 4 completed extract, load, and dbt, then failed when Task 4 opened the database read-only. dbt keeps a read-write connection open in the same process, and DuckDB refuses a second connection with different settings. Task 4 now connects with default settings; run 5 passed 4 minutes later.

## 7. Task 3: dbt

`dbt build` runs inside the flow (`dbt_build`, using `PrefectDbtRunner` as in `DBT-REFERENCE.md`). It runs `dbt deps`, then builds every model and runs every test, and raises if any model or test fails. This is the only task that runs dbt. Staging models are views; marts are tables, so the tests read stored results (`dbt_project.yml`). Last run: `PASS=51 ERROR=0` (8 models, 43 tests) in about 4 s.

### Staging models (one per raw table)

| Model               | Columns (from `payload`)                                                                                | Rows dropped                          |
|---------------------|---------------------------------------------------------------------------------------------------------|---------------------------------------|
| `stg_commits`       | `sha`, `author_login`, `committed_at` (from `commit.author.date`), `is_bot`                             | none                                  |
| `stg_pull_requests` | `number`, `author_login`, `state`, created/updated/closed/merged times, `is_bot`                        | none                                  |
| `stg_pr_reviews`    | `id`, `pr_number` (parsed from `pull_request_url`), `reviewer_login`, `state`, `submitted_at`, `is_bot` | `PENDING` reviews (no `submitted_at`) |
| `stg_releases`      | `id`, `tag_name`, `published_at`, `prerelease`                                                          | drafts                                |

- Timestamps are cast with `::timestamptz` (GitHub returns UTC; the dbt profile sets `TimeZone: UTC`).
- Deduplication: `qualify row_number() over (partition by repo, id order by fetched_at desc) = 1` keeps the newest copy of each record.

**Bot rule:** `is_bot = user.type = 'Bot' OR user.login LIKE '%[bot]'`, applied to the PR author, the reviewer, and the commit author, and wrapped in `coalesce(..., false)`. 23 commits have no linked GitHub account (the commit email isn't tied to a user). Their login is NULL, so without the `coalesce` their `is_bot` would be NULL; with it, they count as not bots.

### Fact marts

**`fct_pull_requests`**: one row per PR created on or after 2024-01-01, with the required flags and durations: `is_merged`, `is_closed_unmerged`, `is_open`, `first_review_at`, `hours_to_first_review`, `reviewed_within_7d`, `hours_to_merge`, `hours_to_close`, `is_stale`, `has_approving_review`.

- A review counts only if it is not by the PR author and not by a bot. A review by a deleted account (NULL login) does not count, because it can't be shown to be a non-author.
- Run time is `max(fetched_at)`, so a rebuild without new data gives the same results.
- `is_stale` = open and not updated for more than 180 days before run time.

**`fct_releases`**: one row per published release on or after 2024-01-01. `days_since_previous` uses `lag()` over all releases before the window filter, so the first release in the window still measures its gap to the one before it. **Added column:** `days_since_previous_full` is the gap to the previous *full* release (NULL for prereleases). Release regularity in Task 5 uses it, because an rc or beta build is not the same event as a release, and counting it would split one gap into two.

### Aggregate marts

**`mart_repo_monthly`**: one row per repo × month, January 2024 through the run month (35 months × 3 repos). The month list comes from `generate_series` crossed with the repos, so a month with no activity shows 0 instead of disappearing. It has the required columns, plus two:

- `month_index` (0 = January 2024): the x-axis for the trend slopes.
- `top_author_share`: the share of the month's merged non-bot PRs written by the busiest author. It is NULL when a month has no merges, so an empty month doesn't look perfectly spread out.

**`mart_health_inputs`**: one row per repo, one column per Task 5 metric (section 9). Every column is a share, ratio, or slope, never a raw count. Tasks 4–6 read durations and rates from these marts; no Python code recomputes them.

### How medians treat open PRs

Open PRs are kept in `fct_pull_requests` with `is_open = true`. They are never dropped and never given a fake close date: their `merged_at` and `closed_at` stay NULL, so `hours_to_merge` and `hours_to_close` are NULL.

`median()` skips NULLs. So:
- **`median_merge_hours`** covers only PRs merged that month; open and closed-unmerged PRs drop out.
- **`median_first_review_hours`** covers only reviewed PRs; open PRs without a review drop out.

### Tests (43, plus 8 models = 51 in `dbt build`)

| Type         | Where                                      | What it checks                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Generic (12) | `models/staging/schema.yml`                | `unique` + `not_null` on `sha`, review `id`, release `id`; `(repo, number)` unique for PRs (`dbt_utils.unique_combination_of_columns`) and `number` not null; `committed_at` not null; `relationships` from `stg_pr_reviews.pr_number` to `stg_pull_requests.number`; `accepted_values` on PR `state` (`open`, `closed`) and review `state` (`APPROVED`, `CHANGES_REQUESTED`, `COMMENTED`, `DISMISSED`) |
| Generic (29) | `models/marts/schema.yml`                  | Unique keys: `(repo, number)`, `(repo, month)`, release `id`, `repo` in `mart_health_inputs`; `not_null` on keys, dates, and every Task 5 metric; `accepted_values` on PR `state`; `dbt_utils.accepted_range`: durations and release gaps ≥ 0, shares and ratios between 0 and 1                                                                                                                        |
| Singular     | `tests/assert_pr_closed_after_created.sql` | No PR closed or merged before it was created                                                                                                                                                                                                                                                                                                                                                            |
| Singular     | `tests/assert_review_after_pr_created.sql` | No review submitted before its PR was created                                                                                                                                                                                                                                                                                                                                                           |

Results print to the terminal during the run and are saved in `logs/dbt.log`, for example:

```
9 of 51 PASS assert_pr_closed_after_created ......... [PASS in 1.29s]
10 of 51 PASS assert_review_after_pr_created ........ [PASS in 1.38s]
```

`schema.yml` in each folder describes every model and column; the raw tables are declared as dbt sources in `models/staging/sources.yml`.

## 8. Task 4: Validation

`validate` is a Python task (no dbt) that runs after dbt. It runs every check, logs the results, and raises once at the end if any completeness check failed, so one run lists every problem. Numbers below are from the last flow run (2026-11-04 13:33, `logs/validate.log`).

### Completeness checks

| Check                        | How                                                                                                                                 | Last run                                              |
|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Landed files → raw tables    | Every distinct `(repo, id)` in the landed JSON, in `data/raw/` **and** every `data/archive/raw_*/` folder, must be in the raw table | 12 of 12 repo × endpoint pairs: 0 missing             |
| Raw → staging                | Raw row count minus rows staging drops on purpose (`PENDING` reviews, draft releases) must equal the staging row count              | 12 of 12 match (0 dropped rows this run)              |
| Commits every complete month | Every repo has at least one commit in every complete month (the run month is exempt)                                                | 1 zero-commit month, passed by the sanity check below |

**Zero-commit sanity check (an addition to the README rule).** A complete month with 0 commits fails, unless that month also had 0 merged PRs. Every merge adds a commit, so a month with no merges and no commits means an inactive repo, not lost data. It is logged as a warning:

```
WARNING [dask/dask] 2026-10: 0 commits. Passed by the secondary sanity check (0 merged PRs, 64 opened): the repo was inactive that month, not missing data.
```

The archive folders are checked too, so the check covers every record ever landed, not only the latest incremental run's files.

### Outliers

All flagged records are **kept**: none are capped or dropped. They are real merges and real work, and the statistics use medians and 90th percentiles, which a few extreme values barely move.

**Time to merge: 55 PRs flagged.** PRs get separate cutoffs by author type, because a wait that is normal for a person means an automated PR got stuck:

| Author | Cutoff                                                                                             | Why this cutoff                                                                                                                                                                  | dask | plotly.py | scikit-learn |
|--------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|-----------|--------------|
| Human  | Above the repo's 99th percentile of merged human PRs (dask 92.5 days, plotly 333.3, sklearn 302.1) | Only PRs far outside the normal range. The 90th percentile would flag 465 human PRs and the 95th 234; the 90th percentile is already reported as part of the typical range below | 9    | 5         | 34           |
| Bot    | More than 7 days                                                                                   | Bot PRs (dependency updates) normally merge within hours to 3 days                                                                                                               | 3    | 1         | 3            |

Notable: the 5 plotly PRs are all by one author and took 409–492 days, likely a batch of old PRs merged together. The longest overall is scikit-learn #28464 at 777 days.

<details>
<summary>All 55 flagged PRs (from <code>logs/validate.log</code>)</summary>

```
[dask/dask] PR #12057 by dependabot[bot] (bot): 134.0 days to merge (cutoff 7.0)
[dask/dask] PR #12328 by dependabot[bot] (bot): 79.8 days to merge (cutoff 7.0)
[dask/dask] PR #12334 by dependabot[bot] (bot): 35.6 days to merge (cutoff 7.0)
[dask/dask] PR #10812 by joanrue (human): 215.3 days to merge (cutoff 92.5)
[dask/dask] PR #11118 by phofl (human): 160.5 days to merge (cutoff 92.5)
[dask/dask] PR #12010 by DimitriPapadopoulos (human): 118.3 days to merge (cutoff 92.5)
[dask/dask] PR #12002 by DimitriPapadopoulos (human): 115.8 days to merge (cutoff 92.5)
[dask/dask] PR #11965 by faulaire (human): 115.2 days to merge (cutoff 92.5)
[dask/dask] PR #12024 by DimitriPapadopoulos (human): 96.6 days to merge (cutoff 92.5)
[dask/dask] PR #11128 by dstansby (human): 93.8 days to merge (cutoff 92.5)
[dask/dask] PR #12041 by DimitriPapadopoulos (human): 93.8 days to merge (cutoff 92.5)
[dask/dask] PR #11875 by rjzamora (human): 92.9 days to merge (cutoff 92.5)
[plotly/plotly.py] PR #5775 by dependabot[bot] (bot): 13.8 days to merge (cutoff 7.0)
[plotly/plotly.py] PR #5232 by robertoffmoura (human): 491.5 days to merge (cutoff 333.3)
[plotly/plotly.py] PR #5311 by robertoffmoura (human): 426.6 days to merge (cutoff 333.3)
[plotly/plotly.py] PR #5262 by robertoffmoura (human): 426.4 days to merge (cutoff 333.3)
[plotly/plotly.py] PR #5310 by robertoffmoura (human): 425.9 days to merge (cutoff 333.3)
[plotly/plotly.py] PR #5285 by robertoffmoura (human): 409.2 days to merge (cutoff 333.3)
[scikit-learn/scikit-learn] PR #33917 by dependabot[bot] (bot): 18.6 days to merge (cutoff 7.0)
[scikit-learn/scikit-learn] PR #29981 by dependabot[bot] (bot): 8.1 days to merge (cutoff 7.0)
[scikit-learn/scikit-learn] PR #32981 by dependabot[bot] (bot): 7.1 days to merge (cutoff 7.0)
[scikit-learn/scikit-learn] PR #28464 by marikabergengren (human): 777.1 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29026 by hchau630 (human): 693.6 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28760 by jeremiedbb (human): 687.1 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29532 by blitchj (human): 626.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29499 by ArturoAmorQ (human): 626.4 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29641 by snath-xoc (human): 571.5 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28043 by StefanieSenger (human): 540.9 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29661 by EmilyXinyi (human): 512.2 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30869 by adrinjalali (human): 501.2 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29155 by agitter (human): 491.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #31413 by adrinjalali (human): 488.1 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30859 by adrinjalali (human): 454.1 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28971 by OmarManzoor (human): 450.0 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29097 by lorentzenchr (human): 445.3 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28755 by marijavlajic (human): 441.9 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29307 by maf-rnmourao (human): 439.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30508 by lucyleeow (human): 431.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29022 by deepyaman (human): 412.9 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #32042 by lorentzenchr (human): 406.6 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #31127 by lorentzenchr (human): 400.5 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29310 by icfaust (human): 400.3 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28559 by myenugula (human): 391.0 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30134 by SuccessMoses (human): 378.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30751 by snath-xoc (human): 368.6 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #28981 by cynddl (human): 349.6 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29651 by Shashank1202 (human): 348.9 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #31177 by dschult (human): 334.2 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #31289 by EngineerDanny (human): 329.8 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #32789 by nandan2003 (human): 322.1 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29151 by ArturoAmorQ (human): 320.2 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30041 by ogrisel (human): 316.3 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #32587 by cakedev0 (human): 314.8 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #29716 by AdrianoCLeao (human): 310.7 days to merge (cutoff 302.1)
[scikit-learn/scikit-learn] PR #30564 by ogrisel (human): 303.5 days to merge (cutoff 302.1)
```

</details>

**Weekly commits: 4 weeks flagged.** Cutoff: above Q3 + 3 × IQR of that repo's weekly commit counts (Tukey's "far out" rule). Bot commits are counted so an automated burst would show; none of the 4 weeks had any.

| Repo         | Week of    | Commits | Bot commits | Cutoff |
|--------------|------------|---------|-------------|--------|
| dask         | 2024-01-08 | 77      | 0           | 46     |
| dask         | 2024-01-15 | 75      | 0           | 46     |
| plotly.py    | 2024-10-21 | 98      | 0           | 93     |
| scikit-learn | 2025-10-06 | 83      | 0           | 53     |

### Descriptive statistics

Read from `fct_pull_requests`, `mart_repo_monthly`, and `fct_releases`; no durations are recomputed. Medians and percentiles skip PRs that were never reviewed or never merged (section 7).

| Repo         | First review p50 (h) | First review p90 (h) | Merge p50 (h) | Merge p90 (h) | Commits / week | Releases / year |
|--------------|----------------------|----------------------|---------------|---------------|----------------|-----------------|
| dask         | 12.3                 | 211.5                | 12.3          | 216.8         | 9.3            | 15.5            |
| plotly.py    | 16.1                 | 693.2                | 41.3          | 1,130.1       | 19.1           | 8.8             |
| scikit-learn | 13.1                 | 275.2                | 36.6          | 837.5         | 22.6           | 5.3             |

Releases per year count full releases only (prereleases excluded, as in section 7), divided by the time from 2024-01-01 to the run.

**Note on dask's releases per year.** dask's GitHub Releases start on 2024-04-19; earlier versions were published as git tags only, which the Releases endpoint does not return. Its 44 releases are therefore divided by a period about 3.5 months longer than the one they cover, so 15.5 per year **understates** its rate. Measured from its first GitHub Release it is 17.3 per year. dask has the most releases per year either way. Release regularity (Task 5) is not affected, because it uses the gaps between releases, not a rate.

## 9. Task 5: Health model and ranking

_To be written._

## 10. Task 6: Plots

_To be written._

## 11. Data findings and limitations

_To be written._

## 12. Extended items

_To be written after the scheduled deployment and API odometer are built._

## 13. Tests

_To be written._
