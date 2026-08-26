with source_data as (
    select
        cast(inference_id as varchar) as inference_id,
        cast(completion_id as varchar) as completion_id,
        cast(user_id as varchar) as user_id,
        cast(model_id as varchar) as model_id,
        try_cast(request_at as timestamp) as request_at,
        try_cast(response_at as timestamp) as response_at,
        try_cast(latency_ms as bigint) as latency_ms,
        try_cast(input_tokens as bigint) as input_tokens,
        try_cast(output_tokens as bigint) as output_tokens,
        cast(inference_status as varchar) as inference_status,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'model_inferences') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by inference_id
            order by ingested_at asc, physical_partition_date asc
        ) as replay_rank
    from source_data
)

select
    inference_id,
    completion_id,
    user_id,
    model_id,
    request_at,
    response_at,
    latency_ms,
    input_tokens,
    output_tokens,
    inference_status,
    ingested_at,
    physical_partition_date,
    model_id is null as is_model_id_missing,
    case
        when response_at is null then null
        else response_at >= request_at
    end as is_timestamp_order_valid
from ranked
where replay_rank = 1
