-- Staging: one row per published release. Drafts are dropped.

with source as (
    select * from {{ source('raw', 'raw_releases') }}
),

renamed as (
    select
        repo,
        id,
        payload->>'tag_name'                     as tag_name,
        (payload->>'published_at')::timestamptz  as published_at,
        (payload->>'prerelease')::boolean        as prerelease,
        (payload->>'draft')::boolean             as draft,
        fetched_at
    from source
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select *
from renamed
where not draft
