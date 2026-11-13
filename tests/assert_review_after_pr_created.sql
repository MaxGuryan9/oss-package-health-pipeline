-- INTERVIEW: GOAL OF THIS TEST: the second custom SQL test required by Task 3 'Singular tests'. It joins each review
-- to its PR and returns any review submitted before the PR was created; 0 rows means pass. CATCHES: a review attached
-- to the wrong PR (for example a pr_number parsed wrongly from pull_request_url) or a timestamp cast in the wrong
-- time zone.
-- Singular test: fails if any review was submitted before its PR was created.

select r.repo, r.id, r.pr_number, r.submitted_at, p.created_at
from {{ ref('stg_pr_reviews') }} r
join {{ ref('stg_pull_requests') }} p
    on p.repo = r.repo
    and p.number = r.pr_number
where r.submitted_at < p.created_at
