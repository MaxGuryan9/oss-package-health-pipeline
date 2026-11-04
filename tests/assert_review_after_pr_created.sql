-- Singular test: fails if any review was submitted before its PR was created.

select r.repo, r.id, r.pr_number, r.submitted_at, p.created_at
from {{ ref('stg_pr_reviews') }} r
join {{ ref('stg_pull_requests') }} p
    on p.repo = r.repo
    and p.number = r.pr_number
where r.submitted_at < p.created_at
