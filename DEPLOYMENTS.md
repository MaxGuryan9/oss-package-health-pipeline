# Deployment record

**Deployment command** (run from the project root; the Prefect Secret block `rhk2dx-pat` must exist on the server):

```bash
python main.py --deploy
```

This calls `deploy()` in `main.py`, which follows the [Prefect deployment instructions](docs/course/DEPLOYMENT-INSTRUCTIONS.md): `from_source()` with a private `GitRepository` (branch `main`, credentials from the `rhk2dx-pat` Secret block), then `.deploy()`.

| Setting         | Value                                                                                                                       |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------|
| Deployment      | `rhk2dx-package-health-flow/rhk2dx-deployment`                                                                              |
| Entrypoint      | `main.py:package_health_flow`                                                                                               |
| Schedule        | `Cron("0 0 28 * *", timezone="America/New_York")`; next runs 2026-11-28, 2026-12-28, 2027-01-28                             |
| Work pool       | `uvasds-k8s-pool`                                                                                                           |
| `job_variables` | `cpu_request: "2"`, `cpu_limit: "2"`, `memory_request: "2Gi"`, `memory_limit: "4Gi"`, plus `env: {PREFECT_LOGGING_EXTRA_LOGGERS: "extract,load,transform,validate,model,plot"}` |

**Naming:** the `rhk2dx-` prefix on the flow, deployment, and Secret block names keeps them unique on a shared Prefect server. The names stay as they are so the existing deployment keeps running.

**Why the extra `env` setting:** `main.py` sets `PREFECT_LOGGING_EXTRA_LOGGERS` before importing Prefect so each task's log lines reach Prefect. That works locally, but on the server Prefect is already running when `main.py` loads, so the first manual run (`peridot-lobster`) showed none of our log lines. It was cancelled to save API budget, the setting was added to the pod's environment, and the run below was started.

**Every server run is a full run.** Each run starts a new pod with a fresh clone. `data/` and `packages.duckdb` are not in the repo, so there is no `_load_state` and the flow fetches the whole window (about 7,000 API requests). The flow itself is incremental, and local runs need only 13–61 requests. Keeping runs incremental on the server would require storing `data/` and `packages.duckdb` in persistent storage, which, like the plots, is out of scope here.

## Manually triggered run: `whimsical-peacock`

Started with the **Run** button on 2026-11-04 at 22:06 EST (03:06 UTC). Times below are UTC. Main flow run:

```
03:06:38  Kubernetes job 'whimsical-peacock-4qprh' created in namespace 'uvasds-services'
03:06:42  Executing deployment step: git_clone
03:06:43  Deployment step 'git_clone' completed successfully
03:06:47  Beginning flow run 'whimsical-peacock' for flow 'rhk2dx-package-health-flow'
```

`extract` subflow (`outgoing-oarfish`):

```
03:06:49  Extract starting (resume): 3 repos, window from 2024-01-01T00:00:00Z
03:06:50  [plotly/plotly.py] pulls page 1: 100 records (budget left 1048)
03:07:43  [scikit-learn/scikit-learn] pulls page 43: 100 records (budget left 711)
03:09:00  [dask/dask] reviews: 400/1233 PRs (32%)
03:09:02  Rate limit: 25 left; sleeping 2059s until reset
```

The run started with 1,048 requests left in the hour, because the cancelled run had used the rest. It used them, then slept until GitHub's reset (22:43 EST), as designed. A full run needs about 7,000 requests, so it sleeps once more before it finishes.

The rest of the run (UTC):

```
04:05:51  Rate limit: 25 left; sleeping 2265s until reset
04:51:53  Extract finished: 7327 API requests this run
04:51:55  raw_commits: 0 rows before, 7566 after
04:52:00  raw_pull_requests: 0 rows before, 8800 after
04:52:03  raw_pr_reviews: 0 rows before, 21921 after
04:52:04  raw_releases: 0 rows before, 204 after
04:52:25  dbt build finished: 51 models and tests passed
04:52:27  All completeness checks passed
04:52:27    1    plotly/plotly.py            0.673
04:52:27    2    scikit-learn/scikit-learn   0.615
04:52:27    3    dask/dask                   0.550
04:52:32  Saved .../plots/dimension_profile.png (and monthly_trends.png, load_vs_rejection.png)
04:52:32  API odometer: 7327 GitHub API requests this flow run
```

**Final state: Completed** at 04:52:32 UTC (23:52 EST), 1 h 46 min after the start, about 72 minutes of it rate-limit sleep. A fresh clone with only the token from the Secret block ran all six tasks end to end. The odometer (7,327) matches extract's own count. The full log is on the shared Prefect server under run `whimsical-peacock`.
