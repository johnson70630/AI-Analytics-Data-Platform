with ordered_versions as (
    select
        user_id,
        user_key,
        effective_start_at,
        effective_end_at,
        is_current,
        lag(effective_end_at) over (
            partition by user_id
            order by effective_start_at, user_key
        ) as previous_effective_end_at
    from {{ ref('dim_user') }}
)

select
    user_id,
    user_key,
    effective_start_at,
    effective_end_at,
    is_current
from ordered_versions
where effective_start_at is null
   or effective_end_at <= effective_start_at
   or effective_start_at < previous_effective_end_at
   or (is_current and effective_end_at is not null)
   or (not is_current and effective_end_at is null)
