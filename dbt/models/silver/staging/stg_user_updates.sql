with source_data as (
    select
        cast(update_id as varchar) as update_id,
        cast(user_id as varchar) as user_id,
        cast(field_name as varchar) as field_name,
        cast(old_value as varchar) as old_value,
        cast(new_value as varchar) as new_value,
        {{ staging_cast('updated_at', 'timestamp') }} as updated_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'user_updates') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by update_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    update_id,
    user_id,
    field_name,
    old_value,
    new_value,
    updated_at,
    ingested_at,
    physical_partition_date
from ranked
where replay_rank = 1
