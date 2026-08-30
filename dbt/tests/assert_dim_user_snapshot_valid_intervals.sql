with ordered_snapshot as (
    select
        user_id,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        lag(dbt_valid_to) over (
            partition by user_id
            order by dbt_valid_from, dbt_scd_id
        ) as previous_valid_to
    from {{ ref('dim_user_snapshot') }}
)

select
    user_id,
    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to
from ordered_snapshot
where dbt_valid_from is null
   or dbt_valid_to <= dbt_valid_from
   or dbt_valid_from < previous_valid_to
