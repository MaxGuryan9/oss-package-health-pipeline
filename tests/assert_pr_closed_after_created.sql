-- INTERVIEW: GOAL OF THIS TEST: one of the two custom SQL tests required by Task 3 'Singular tests'. A dbt singular
-- test passes when the query returns 0 rows, so this selects the bad rows: PRs closed or merged before they were
-- created. It runs inside `dbt build`, so a bad row fails the flow. Decision: it reads stg_pull_requests, so all
-- staged PRs are checked, not only the 2024+ PRs in the fact table.
-- Singular test: fails if any PR was closed or merged before it was created.
-- Checks every staged PR, not only the 2024+ PRs in fct_pull_requests.

select repo, number, created_at, closed_at, merged_at
from {{ ref('stg_pull_requests') }}
where closed_at < created_at
    or merged_at < created_at
