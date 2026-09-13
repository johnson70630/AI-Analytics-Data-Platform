with source_data as (
    select
        cast(feedback_id as varchar) as feedback_id,
        cast(completion_id as varchar) as completion_id,
        cast(user_id as varchar) as user_id,
        cast(feedback_type as varchar) as feedback_type,
        {{ staging_cast('feedback_score', 'integer') }} as feedback_score,
        {{ staging_cast('created_at', 'timestamp') }} as created_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'feedback') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by feedback_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    feedback_id,
    completion_id,
    user_id,
    feedback_type,
    feedback_score,
    created_at,
    ingested_at,
    physical_partition_date,
    feedback_type is null as is_feedback_type_missing
from ranked
where replay_rank = 1
