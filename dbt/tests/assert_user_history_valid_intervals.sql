with ordered_history as (
    select
        *,
        lag(effective_end_at) over (
            partition by user_id
            order by version_sequence
        ) as previous_effective_end_at,
        count(*) over (
            partition by user_id, version_sequence
        ) as version_identity_count
    from {{ ref('user_state_history') }}
)

select
    user_id,
    version_sequence,
    effective_start_at,
    effective_end_at,
    is_current
from ordered_history
where version_identity_count != 1
   or effective_start_at is null
   or (is_current and effective_end_at is not null)
   or (not is_current and effective_end_at is null)
   or effective_end_at <= effective_start_at
   or effective_start_at < previous_effective_end_at
