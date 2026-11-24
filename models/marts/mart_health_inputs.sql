-- Mart: one row per repo, one column per metric scored by `score_health`.
-- Every column is a share, ratio or slope, never a raw count, so a large repo
-- cannot win by volume alone. Direction (which way is healthier) is in schema.yml.

with run_time as (
    select max(fetched_at)::timestamptz as run_at
    from {{ ref('stg_pull_requests') }}
),

-- responsiveness, throughput and backlog, from one row per PR
prs as (
    select
        prs.repo,
        avg(case when date_diff('second', prs.created_at, run_time.run_at) / 86400.0 >= 7
                 then prs.reviewed_within_7d::int end)                   as pct_reviewed_within_7d,
        sum(prs.is_merged::int)
            / nullif(sum(prs.is_merged::int) + sum(prs.is_closed_unmerged::int), 0)
                                                                         as merge_rate,
        sum(prs.is_stale::int) / nullif(sum(prs.is_open::int), 0)       as pct_open_stale
    from {{ ref('fct_pull_requests') }} prs
    cross join run_time
    group by all
),

-- release cadence: spread of the gaps between releases relative to their average
releases as (
    select
        repo,
        stddev_samp(days_since_previous_full) / nullif(avg(days_since_previous_full), 0)  as release_gap_cv
    from {{ ref('fct_releases') }}
    group by all
),

-- activity and contributor trends over complete months only
monthly as (
    select
        repo,
        regr_slope(commits, month_index) / nullif(avg(commits), 0)  as commit_trend,
        regr_slope(top_author_share, month_index)                   as top_author_share_trend
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
    group by all
)

select
    prs.repo,
    prs.pct_reviewed_within_7d,
    prs.merge_rate,
    prs.pct_open_stale,
    releases.release_gap_cv,
    monthly.commit_trend,
    monthly.top_author_share_trend
from prs
left join releases using (repo)
left join monthly using (repo)
