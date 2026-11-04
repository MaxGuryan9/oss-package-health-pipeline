-- Staging: one row per commit, typed columns pulled out of the JSON payload.
-- Bot rule: is_bot = author.type = 'Bot' OR author.login LIKE '%[bot]'.
-- author is null when the commit email isn't linked to a GitHub account; is_bot is then false.

with source as (
    select * from {{ source('raw', 'raw_commits') }}
),

renamed as (
    select
        repo,
        id                                                   as sha,
        payload->'author'->>'login'                          as author_login,
        (payload->'commit'->'author'->>'date')::timestamptz  as committed_at,
        coalesce(
            (payload->'author'->>'type') = 'Bot'
            or (payload->'author'->>'login') like '%[bot]',
            false
        )                                                    as is_bot,
        fetched_at
    from source
    -- keep the latest fetch of each record
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select * from renamed
