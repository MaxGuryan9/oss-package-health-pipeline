-- Staging: one row per submitted review. PENDING reviews are dropped (no submitted_at).
-- Bot rule applied to the reviewer (user); 3 reviews have a null user, so is_bot is false.

with source as (
    select * from {{ source('raw', 'raw_pr_reviews') }}
),

renamed as (
    select
        repo,
        id,
        regexp_extract(payload->>'pull_request_url', '(\d+)$', 1)::bigint  as pr_number,
        payload->'user'->>'login'                                          as reviewer_login,
        payload->>'state'                                                  as state,
        (payload->>'submitted_at')::timestamptz                            as submitted_at,
        coalesce(
            (payload->'user'->>'type') = 'Bot'
            or (payload->'user'->>'login') like '%[bot]',
            false
        )                                                                  as is_bot,
        fetched_at
    from source
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select *
from renamed
where state != 'PENDING'
