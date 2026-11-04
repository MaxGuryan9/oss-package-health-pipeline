-- Fact: one row per published release on or after 2024-01-01.
-- lag() runs over ALL releases before the window filter, so the first release
-- in the window still measures its gap to the release before it.

with releases as (
    select * from {{ ref('stg_releases') }}
),

with_gaps as (
    select
        repo,
        id,
        tag_name,
        published_at,
        prerelease as is_prerelease,
        date_diff(
            'day',
            lag(published_at) over (partition by repo order by published_at),
            published_at
        ) as days_since_previous
    from releases
)

select *
from with_gaps
where published_at >= '2024-01-01'
