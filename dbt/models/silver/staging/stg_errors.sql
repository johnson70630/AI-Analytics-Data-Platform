with source_data as (
    select
        cast(error_id as varchar) as error_id,
        cast(user_id as varchar) as user_id,
        cast(conversation_id as varchar) as conversation_id,
        cast(message_id as varchar) as message_id,
        cast(completion_id as varchar) as completion_id,
        cast(inference_id as varchar) as inference_id,
        cast(model_id as varchar) as model_id,
        cast(error_source as varchar) as error_source,
        cast(error_type as varchar) as error_type,
        cast(error_code as varchar) as error_code,
        cast(severity as varchar) as severity,
        try_cast(occurred_at as timestamp) as occurred_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'errors') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by error_id
            order by ingested_at asc, physical_partition_date asc
        ) as replay_rank
    from source_data
)

select
    error_id,
    user_id,
    conversation_id,
    message_id,
    completion_id,
    inference_id,
    model_id,
    error_source,
    error_type,
    error_code,
    severity,
    occurred_at,
    ingested_at,
    physical_partition_date
from ranked
where replay_rank = 1
