with source_data as (
    select
        {{ staging_cast('conversation_id', 'varchar') }} as conversation_id,
        {{ staging_cast('user_id', 'varchar') }} as user_id,
        {{ staging_cast('created_at', 'timestamp') }} as created_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'conversations') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by conversation_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    conversation_id,
    user_id,
    created_at,
    ingested_at,
    physical_partition_date
from ranked
where replay_rank = 1
