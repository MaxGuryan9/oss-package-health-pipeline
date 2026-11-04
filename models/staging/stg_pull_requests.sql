-- Staging: one row per pull request. Bot rule applied to the PR author (user).

with source as (
    select * from {{ source('raw', 'raw_pull_requests') }}
),

renamed as (
    select
        repo,
        id                                     as number,
        payload->'user'->>'login'              as author_login,
        payload->>'state'                      as state,
        (payload->>'created_at')::timestamptz  as created_at,
        (payload->>'updated_at')::timestamptz  as updated_at,
        (payload->>'closed_at')::timestamptz   as closed_at,
        (payload->>'merged_at')::timestamptz   as merged_at,
        coalesce(
            (payload->'user'->>'type') = 'Bot'
            or (payload->'user'->>'login') like '%[bot]',
            false
        )                                      as is_bot,
        fetched_at
    from source
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select * from renamed
