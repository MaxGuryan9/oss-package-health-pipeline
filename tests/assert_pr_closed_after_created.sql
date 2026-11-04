-- Singular test: fails if any PR was closed or merged before it was created.
-- Checks every staged PR, not only the 2024+ PRs in fct_pull_requests.

select repo, number, created_at, closed_at, merged_at
from {{ ref('stg_pull_requests') }}
where closed_at < created_at
    or merged_at < created_at
