-- Mart: one row per repo x month, January 2024 through the run month.
-- The month list (date spine) is built first and crossed with the repos, so a month
-- with no activity shows 0 instead of disappearing.
-- Each event counts in the month it happened: opened by created_at, merged by merged_at,
-- closed-unmerged by closed_at. median() skips NULLs, so unreviewed PRs drop out of
-- median_first_review_hours and unmerged PRs drop out of median_merge_hours.

with run_time as (
    select max(fetched_at)::timestamptz as run_at
    from {{ ref('stg_pull_requests') }}
),

months as (
    select unnest(generate_series(
        date '2024-01-01',
        date_trunc('month', run_at)::date,
        interval 1 month
    ))::date as month
    from run_time
),

repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
),

spine as (
    select
        repos.repo,
        months.month,
        months.month = date_trunc('month', run_time.run_at)::date as is_partial_month
    from repos
    cross join months
    cross join run_time
),

opened as (
    select
        repo,
        date_trunc('month', created_at)::date                         as month,
        count(*)                                                      as prs_opened,
        median(hours_to_first_review)                                 as median_first_review_hours,
        count(distinct case when not author_is_bot then author_login end) as active_contributors
    from {{ ref('fct_pull_requests') }}
    group by all
),

merged as (
    select
        repo,
        date_trunc('month', merged_at)::date  as month,
        count(*)                              as prs_merged,
        median(hours_to_merge)                as median_merge_hours
    from {{ ref('fct_pull_requests') }}
    where is_merged
    group by all
),

closed_unmerged as (
    select
        repo,
        date_trunc('month', closed_at)::date  as month,
        count(*)                              as prs_closed_unmerged
    from {{ ref('fct_pull_requests') }}
    where is_closed_unmerged
    group by all
),

-- merged PRs per non-bot author per month, for the top author's share
merged_by_author as (
    select
        repo,
        date_trunc('month', merged_at)::date  as month,
        author_login,
        count(*)                              as prs
    from {{ ref('fct_pull_requests') }}
    where is_merged
        and not author_is_bot
    group by all
),

top_author as (
    select
        repo,
        month,
        max(prs) / sum(prs)  as top_author_share
    from merged_by_author
    group by all
),

commits as (
    select
        repo,
        date_trunc('month', committed_at)::date  as month,
        count(*)                                 as commits
    from {{ ref('stg_commits') }}
    group by all
),

filled as (
    select
        spine.repo,
        spine.month,
        date_diff('month', date '2024-01-01', spine.month)  as month_index,
        coalesce(opened.prs_opened, 0)                      as prs_opened,
        coalesce(merged.prs_merged, 0)                      as prs_merged,
        coalesce(closed_unmerged.prs_closed_unmerged, 0)    as prs_closed_unmerged,
        opened.median_first_review_hours,
        merged.median_merge_hours,
        coalesce(commits.commits, 0)                        as commits,
        coalesce(opened.active_contributors, 0)             as active_contributors,
        top_author.top_author_share,
        spine.is_partial_month
    from spine
    left join opened
        on opened.repo = spine.repo and opened.month = spine.month
    left join merged
        on merged.repo = spine.repo and merged.month = spine.month
    left join closed_unmerged
        on closed_unmerged.repo = spine.repo and closed_unmerged.month = spine.month
    left join commits
        on commits.repo = spine.repo and commits.month = spine.month
    left join top_author
        on top_author.repo = spine.repo and top_author.month = spine.month
)

select
    *,
    prs_merged / nullif(prs_merged + prs_closed_unmerged, 0) as merge_ratio
from filled
