-- Fact: one row per PR created on or after 2024-01-01, with durations and flags computed once here.
-- Open PRs are kept (is_open = true); their merge and close durations stay NULL, never a fake close date.
-- Run time = max(fetched_at), so a rebuild without new data gives the same results.

with prs as (
    select * from {{ ref('stg_pull_requests') }}
    where created_at >= '2024-01-01'
),

run_time as (
    select max(fetched_at)::timestamptz as run_at
    from {{ ref('stg_pull_requests') }}
),

-- first review and approval per PR, excluding the PR author's own reviews and bot reviews
reviews as (
    select
        prs.repo,
        prs.number,
        min(r.submitted_at)                                         as first_review_at,
        max(case when r.state = 'APPROVED' then 1 else 0 end) = 1   as has_approving_review
    from prs
    join {{ ref('stg_pr_reviews') }} r
        on r.repo = prs.repo
        and r.pr_number = prs.number
    where not r.is_bot
        and r.reviewer_login != prs.author_login
    group by all
),

joined as (
    select
        prs.repo,
        prs.number,
        prs.author_login,
        prs.is_bot                                                         as author_is_bot,
        prs.state,
        prs.created_at,
        prs.updated_at,
        prs.closed_at,
        prs.merged_at,
        prs.merged_at is not null                                          as is_merged,
        prs.state = 'closed' and prs.merged_at is null                     as is_closed_unmerged,
        prs.state = 'open'                                                 as is_open,
        reviews.first_review_at,
        date_diff('second', prs.created_at, reviews.first_review_at) / 3600.0  as hours_to_first_review,
        date_diff('second', prs.created_at, prs.merged_at) / 3600.0            as hours_to_merge,
        date_diff('second', prs.created_at, prs.closed_at) / 3600.0            as hours_to_close,
        prs.state = 'open'
            and date_diff('second', prs.updated_at, run_time.run_at) / 86400.0 > 180
                                                                           as is_stale,
        coalesce(reviews.has_approving_review, false)                      as has_approving_review
    from prs
    cross join run_time
    left join reviews
        on reviews.repo = prs.repo
        and reviews.number = prs.number
)

select
    *,
    coalesce(hours_to_first_review <= 168, false) as reviewed_within_7d
from joined
