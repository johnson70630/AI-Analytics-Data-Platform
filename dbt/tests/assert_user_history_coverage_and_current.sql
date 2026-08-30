with source_users as (
    select user_id
    from {{ ref('stg_users') }}
),

history_by_user as (
    select
        user_id,
        count(*) as version_count,
        count_if(is_current) as current_count
    from {{ ref('user_state_history') }}
    group by user_id
)

select
    coalesce(source_users.user_id, history.user_id) as user_id,
    history.version_count,
    history.current_count
from source_users
full outer join history_by_user as history
    on source_users.user_id = history.user_id
where source_users.user_id is null
   or history.user_id is null
   or history.current_count != 1
