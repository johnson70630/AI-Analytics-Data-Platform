with expected_by_user as (
    select
        users.user_id,
        1 + {{ count_if(
            "updates.field_name in ('country_code', 'account_status') and "
            ~ "updates.old_value is distinct from updates.new_value"
        ) }} as expected_version_count
    from {{ ref('stg_users') }} as users
    left join {{ ref('stg_user_updates') }} as updates
        on users.user_id = updates.user_id
    group by users.user_id
),

actual_by_user as (
    select
        user_id,
        count(*) as actual_version_count
    from {{ ref('user_state_history') }}
    group by user_id
)

select
    expected.user_id,
    expected.expected_version_count,
    actual.actual_version_count
from expected_by_user as expected
left join actual_by_user as actual
    on expected.user_id = actual.user_id
where actual.actual_version_count is distinct from expected.expected_version_count
