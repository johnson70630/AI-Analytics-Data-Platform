with source_data as (
    select
        cast(feedback_id as varchar) as feedback_id,
        cast(completion_id as varchar) as completion_id,
        cast(user_id as varchar) as user_id,
        cast(feedback_type as varchar) as feedback_type,
        try_cast(feedback_score as integer) as feedback_score,
        try_cast(created_at as timestamp) as created_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'feedback') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by feedback_id
            order by ingested_at asc, physical_partition_date asc
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
