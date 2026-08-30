with snapshot_boundaries as (
    select
        user_id,
        min(dbt_valid_from) as snapshot_start_at
    from {{ ref('dim_user_snapshot') }}
    group by user_id
),

bootstrap_versions as (
    select
        history.user_id,
        current_state.email,
        current_state.name,
        history.country_code,
        history.account_status,
        current_state.signup_at,
        current_state.signup_source,
        history.effective_start_at,
        case
            when history.effective_end_at is null
                or history.effective_end_at > boundary.snapshot_start_at
                then boundary.snapshot_start_at
            else history.effective_end_at
        end as effective_end_at,
        'historical_bootstrap' as version_source
    from {{ ref('user_state_history') }} as history
    inner join snapshot_boundaries as boundary
        on history.user_id = boundary.user_id
    inner join {{ ref('current_user_state') }} as current_state
        on history.user_id = current_state.user_id
    where history.effective_start_at < boundary.snapshot_start_at
),

snapshot_versions as (
    select
        snapshot.user_id,
        current_state.email,
        current_state.name,
        snapshot.country_code,
        snapshot.account_status,
        current_state.signup_at,
        current_state.signup_source,
        snapshot.dbt_valid_from as effective_start_at,
        snapshot.dbt_valid_to as effective_end_at,
        'dbt_snapshot' as version_source
    from {{ ref('dim_user_snapshot') }} as snapshot
    inner join {{ ref('current_user_state') }} as current_state
        on snapshot.user_id = current_state.user_id
),

combined_versions as (
    select * from bootstrap_versions

    union all

    select * from snapshot_versions
)

select
    md5(
        user_id || '|' || cast(effective_start_at as varchar)
    ) as user_key,
    user_id,
    email,
    name,
    country_code,
    account_status,
    signup_at,
    signup_source,
    effective_start_at,
    effective_end_at,
    effective_end_at is null as is_current,
    version_source
from combined_versions
