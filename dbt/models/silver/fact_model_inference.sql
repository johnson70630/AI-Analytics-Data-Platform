select
    inferences.inference_id,
    inferences.completion_id,
    users.user_key,
    count(users.user_key) over (
        partition by inferences.inference_id
    ) as user_key_match_count,
    models.model_key,
    dates.date_key,
    inferences.user_id,
    inferences.model_id,
    inferences.request_at,
    inferences.response_at,
    inferences.latency_ms,
    inferences.input_tokens,
    inferences.output_tokens,
    case
        when inferences.input_tokens is not null
         and inferences.output_tokens is not null
            then inferences.input_tokens + inferences.output_tokens
    end as total_tokens,
    inferences.inference_status,
    inferences.ingested_at,
    inferences.physical_partition_date as source_partition_date,
    inferences.is_model_id_missing,
    inferences.is_timestamp_order_valid
from {{ ref('stg_model_inferences') }} as inferences
left join {{ ref('dim_user') }} as users
    on inferences.user_id = users.user_id
   and inferences.request_at >= users.effective_start_at
   and (
       inferences.request_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_model') }} as models
    on inferences.model_id = models.model_id
left join {{ ref('dim_date') }} as dates
    on cast(inferences.request_at as date) = dates.full_date
