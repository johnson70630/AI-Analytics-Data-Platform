with source_data as (
    select
        cast(conversation_id as varchar) as conversation_id,
        cast(user_id as varchar) as user_id,
        try_cast(created_at as timestamp) as created_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'conversations') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by conversation_id
            order by ingested_at asc, physical_partition_date asc
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
