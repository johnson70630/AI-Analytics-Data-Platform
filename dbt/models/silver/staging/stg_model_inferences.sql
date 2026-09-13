with source_data as (
    select
        cast(inference_id as varchar) as inference_id,
        cast(completion_id as varchar) as completion_id,
        cast(user_id as varchar) as user_id,
        cast(model_id as varchar) as model_id,
        {{ staging_cast('request_at', 'timestamp') }} as request_at,
        {{ staging_cast('response_at', 'timestamp') }} as response_at,
        {{ staging_cast('latency_ms', 'bigint') }} as latency_ms,
        {{ staging_cast('input_tokens', 'bigint') }} as input_tokens,
        {{ staging_cast('output_tokens', 'bigint') }} as output_tokens,
        cast(inference_status as varchar) as inference_status,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'model_inferences') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by inference_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
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
