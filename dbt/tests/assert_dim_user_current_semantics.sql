with source_users as (
    select user_id
    from {{ ref('current_user_state') }}
),

dimension_summary as (
    select
        user_id,
        count_if(is_current) as current_row_count,
        count_if(effective_end_at is null) as open_row_count
    from {{ ref('dim_user') }}
    group by user_id
)

select
    coalesce(source_users.user_id, dimension.user_id) as user_id,
    dimension.current_row_count,
    dimension.open_row_count
from source_users
full outer join dimension_summary as dimension
    on source_users.user_id = dimension.user_id
where source_users.user_id is null
   or dimension.user_id is null
   or dimension.current_row_count != 1
   or dimension.open_row_count != 1
