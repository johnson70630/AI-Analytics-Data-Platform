with snapshot_boundaries as (
    select
        user_id,
        min(dbt_valid_from) as snapshot_start_at
    from {{ ref('dim_user_snapshot') }}
    group by user_id
),

expected_bootstrap as (
    select
        history.user_id,
        history.effective_start_at,
        case
            when history.effective_end_at is null
                or history.effective_end_at > boundary.snapshot_start_at
                then boundary.snapshot_start_at
            else history.effective_end_at
        end as effective_end_at,
        history.country_code,
        history.account_status
    from {{ ref('user_state_history') }} as history
    inner join snapshot_boundaries as boundary
        on history.user_id = boundary.user_id
    where history.effective_start_at < boundary.snapshot_start_at
),

actual_bootstrap as (
    select
        user_id,
        effective_start_at,
        effective_end_at,
        country_code,
        account_status
    from {{ ref('dim_user') }}
    where version_source = 'historical_bootstrap'
)

select
    coalesce(expected.user_id, actual.user_id) as user_id,
    coalesce(expected.effective_start_at, actual.effective_start_at)
        as effective_start_at
from expected_bootstrap as expected
full outer join actual_bootstrap as actual
    on expected.user_id = actual.user_id
   and expected.effective_start_at = actual.effective_start_at
where expected.user_id is null
   or actual.user_id is null
   or expected.effective_end_at is distinct from actual.effective_end_at
   or expected.country_code is distinct from actual.country_code
   or expected.account_status is distinct from actual.account_status
