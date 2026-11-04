# Decisions log (for the README write-up and the oral interview)

Every design decision, why it was made, and where it lives. Append new decisions as they are made.
Not committed (working notes); copy what is needed into README.md.

## Task 1: Extract

| Decision | Why | Where |
|---|---|---|
| Repos: scikit-learn, dask, plotly.py; window from 2024-01-01 | Package set B; README window | `main.py` `REPOS`, `WINDOW_START` |
| Reviews land as `reviews/pr_<n>/page_NNNN.json` (layout A) | One folder per PR makes resume per PR simple | `fetch_reviews` |
| Atomic page writes (`.tmp` then `os.replace`) | A crash never leaves a half-written page that resume would trust | `write_page` |
| Rate-limit buffer of 25 requests; sleep until reset + 5 s | Never hit 0 remaining; small margin for parallel threads | `github_get` |
| 3 repos fetched in parallel, one shared `RATE_LOCK` | One token = one budget; a sleep in one thread pauses all (Concurrent fetching extra) | `extract`, `RATE_LOCK` |
| Ctrl-C sets `STOP`; no retry after Ctrl-C | Clean stop within seconds; matches course `02-load-advanced.py` | `STOP`, `not_after_ctrl_c` |
| Retries `[10, 30, 60]` s on `fetch_repo` | Course pattern; pages on disk are skipped on retry | `fetch_repo` |

## Task 2: Load

| Decision | Why | Where |
|---|---|---|
| `INSERT OR REPLACE` on `PRIMARY KEY (repo, id)` | Re-loading the same files never duplicates rows (idempotent) | `load_table` |
| Incremental extract from `_load_state` watermarks | Never re-download data already loaded (EXTRAS.md point 4) | `read_watermarks`, `extract` |
| **Archive `data/raw` to `data/archive/raw_<UTC>/` instead of deleting** | Keeps every landed file so Task 4 can check all of it; EXTRAS.md allows reworking the resume method | `prepare_raw_dir` |

## Task 3: dbt

| Decision | Why | Where |
|---|---|---|
| Code style priority: project repo examples > course repo > neither | Student must explain every line; untaught structures are avoided | all files |
| `data_tests:` instead of `tests:` | Used by the project repo's example files | both `schema.yml` |
| `profiles.yml`: `threads: 8`, `TimeZone: UTC` | 8 from DBT-REFERENCE.md:43; UTC so GitHub timestamps are not shown in local time | `profiles.yml` |
| Keep `staging/schema.yml` (not in the DBT-REFERENCE layout) | Reviews -> PRs relationship test belongs at staging; README allows extra files | `models/staging/schema.yml` |
| Source `schema: main` | Raw tables live in DuckDB's default `main` schema | `sources.yml` |
| Bot rule coalesced to false | 23 commits have no linked author; `is_bot` must be true/false, not NULL | `stg_*` |
| Reviews by the PR author, bots, and deleted (NULL) users do not count as reviews | README rule; a NULL reviewer cannot be shown to be a non-author | `fct_pull_requests` |
| Stale = open and `date_diff('second') / 86400 > 180` | Exact days without `interval` (not in the course) | `fct_pull_requests` |
| Monthly merge/close counts use PRs created 2024+ only | All PR columns describe the same set of PRs | `mart_repo_monthly` |
| Extra column `month_index` (0 = Jan 2024) | x-axis for the trend slopes | `mart_repo_monthly` |
| Monthly `top_author_share` left NULL when no non-bot merges | A month with no merges must not look perfectly "spread out" (0) | `mart_repo_monthly` |
| **Prereleases kept in data, excluded from release regularity** | rc/beta builds are not the same event as a full release; they split one gap into two. New column `days_since_previous_full`; README's `days_since_previous` unchanged | `fct_releases`, `mart_health_inputs` |
| Marts built as tables | Tests read stored results (build 75 s -> 4 s); dbt replaces them each run (idempotent, DBT-REFERENCE.md:70) | `dbt_project.yml` |
| `PrefectDbtRunner`, runs `deps` then `build` | DBT-REFERENCE.md:74-91; `deps` so a fresh clone works; raises on any failure | `dbt_build` |

## Task 5 metrics (chosen in Task 3, scored in Task 5)

| Metric | Dimension | Healthier | Why chosen |
|---|---|---|---|
| `pct_reviewed_within_7d` | Responsiveness | higher | README example; a share, so size-neutral |
| `merge_rate` | Throughput | higher | Caveat: sklearn's low rate (69%) can reflect strict review of outside PRs |
| `pct_open_stale` | Backlog | lower | Share of the open backlog with no activity in 180+ days |
| `release_gap_cv` (full releases only) | Release cadence | lower | Regularity, not volume |
| `commit_trend` = slope / average monthly commits | Activity (trend) | higher | Required trend metric; dividing by the average stops large repos dominating. Caveats: commits are not all equally meaningful; AI-assisted coding (ASSESSING-HEALTH.md) |
| `top_author_share_trend` | Contributor base (trend) | lower | User added it: a second trend that measures fragility (dependence on one person) without commit_trend's caveats. Not divided by the average because a share is already size-neutral |

- Top author share chosen over bus factor: bus factor is a count that grows with project size (size-normalization criterion).
- Monthly (not quarterly) for the share trend: matches `mart_repo_monthly`, 33-34 points instead of 11.

## Task 4: Validation

| Decision | Why | Where |
|---|---|---|
| **Zero-commit month passes only after a secondary sanity check (0 merged PRs that month)** | A caveat to the README rule "commits in every complete month". Every merge adds a commit, so no merges + no commits = an inactive repo, not lost data. Applies to dask Oct 2026 (0 commits, 0 merges, 64 PRs opened; last merge 2026-09-29). Logged as a warning | `check_monthly_commits` |
| Landed-file check covers `data/raw/` **and** every `data/archive/raw_*/` | All data ever landed is checked, not only the latest incremental run | `landed_globs` |
| Raw vs staging counts allow for intentional drops (PENDING reviews, drafts) | Staging drops these on purpose; currently 0 of each | `STAGING_SPECS` |
| **Merge-time outliers use separate cutoffs per author category** | Goal: account for different categories of PR. A week is normal for a human PR but means an automated (bot) PR got stuck | `log_outliers` |
| Human PRs: above the repo's **99th percentile** | Chosen (not 90 or 95) to make sure every flagged PR is far outside the trend. p90 would flag 464 PRs and is already reported as part of the typical range; p99 flags 48 | `HUMAN_MERGE_QUANTILE` |
| Bot PRs: more than 7 days | Bot PRs normally merge within hours to 3 days; 7 flagged | `BOT_MERGE_HOURS` |
| Weekly commits: above Q3 + 3 x IQR per repo | Tukey's "far out" rule; 4 weeks flagged, all human (0 bot commits) | `COMMIT_BURST_IQR` |
| **All outliers kept** (none capped or dropped) | Real merges and real work; medians and p90 are not distorted by a few extremes | `log_outliers` |
| Releases per year exclude prereleases | Consistent with the release-regularity decision | `log_descriptive_stats` |
| `validate` connects without `read_only=True` | dbt (via `PrefectDbtRunner`) runs in the same process and keeps a read-write connection open; DuckDB refuses a second connection with different settings. Found in the first full-flow run. Task 4 still only runs SELECTs | `validate` |
| Run all checks, then raise once | One run lists every problem; any failure still fails the flow | `validate` |

## Data findings to mention in the write-up

- dask stopped merging after 2026-09-29; its commit_trend is -7.5%/month (R^2 0.46).
- dask GitHub Releases start 2024-04-19 (earlier versions were tags only), so its first in-window release has a NULL gap.
- 7 plotly commits have author dates before 2024 and fall outside the month list.
- plotly: 21 of 45 dependabot PRs are still open (not visible to merge-time outliers; counted in backlog).
- 5 plotly PRs by one author took 409-491 days, likely a batch of old PRs merged together.
- Medians skip NULLs: open and unmerged PRs drop out of merge medians, unreviewed PRs out of review medians (Task 3 "Open items" criterion).

## Task 5: Modeling (decisions pending as of 2026-11-04)

| Question | Options discussed | Status |
|---|---|---|
| Standardization | Relative min-max (best = 1, worst = 0 on every metric; user noted it hides real separation, e.g. dask 55.4% vs plotly 54.7% review rate became 0.08 vs 0.00). Z-score has the same forced spread with 3 repos. Rank keeps only order. **Fixed-range min-max** (fixed meaningful 0/1 bounds, so close repos stay close) proposed | pending |
| Weights | ASSESSING-HEALTH.md order: responsiveness 25, contributor 20, activity 15, release 15, backlog 15, throughput 10; or equal | pending |
| Subscores | 6 dimensions x 1 metric, or 3 grouped dimensions | pending |
| Robustness check | Log the equal-weight ranking next to the weighted one | pending |
