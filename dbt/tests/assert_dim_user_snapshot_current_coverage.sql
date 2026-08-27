with current_state as (
    select user_id
    from {{ ref('current_user_state') }}
),

open_snapshot_rows as (
    select
        user_id,
        count(*) as open_row_count
    from {{ ref('dim_user_snapshot') }}
    where dbt_valid_to is null
    group by user_id
)

select
    coalesce(current_state.user_id, snapshot.user_id) as user_id,
    snapshot.open_row_count
from current_state
full outer join open_snapshot_rows as snapshot
    on current_state.user_id = snapshot.user_id
where current_state.user_id is null
   or snapshot.user_id is null
   or snapshot.open_row_count != 1
