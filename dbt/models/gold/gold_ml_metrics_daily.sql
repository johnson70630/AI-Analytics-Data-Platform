with inference_daily as (
    select
        date_key,
        model_key,
        model_id,
        count(*) as inference_count,
        count_if(inference_status = 'SUCCESS') as successful_inference_count,
        count_if(inference_status = 'FAILED') as failed_inference_count,
        avg(latency_ms) filter (
            where is_timestamp_order_valid and latency_ms >= 0
        ) as average_latency_ms,
        quantile_cont(latency_ms, 0.50) filter (
            where is_timestamp_order_valid and latency_ms >= 0
        ) as p50_latency_ms,
        quantile_cont(latency_ms, 0.95) filter (
            where is_timestamp_order_valid and latency_ms >= 0
        ) as p95_latency_ms,
        quantile_cont(latency_ms, 0.99) filter (
            where is_timestamp_order_valid and latency_ms >= 0
        ) as p99_latency_ms,
        sum(input_tokens) as total_input_tokens,
        sum(output_tokens) as total_output_tokens,
        sum(total_tokens) as total_tokens,
        avg(input_tokens) as average_input_tokens,
        avg(output_tokens) as average_output_tokens
    from {{ ref('fact_model_inference') }}
    group by date_key, model_key, model_id
)

select
    inference.date_key,
    dates.full_date,
    inference.model_key,
    inference.model_id,
    models.model_name,
    models.model_version,
    models.provider,
    inference.inference_count,
    inference.successful_inference_count,
    inference.failed_inference_count,
    inference.successful_inference_count::double
        / nullif(inference.inference_count, 0) as inference_success_rate,
    inference.average_latency_ms,
    inference.p50_latency_ms,
    inference.p95_latency_ms,
    inference.p99_latency_ms,
    inference.total_input_tokens,
    inference.total_output_tokens,
    inference.total_tokens,
    inference.average_input_tokens,
    inference.average_output_tokens
from inference_daily as inference
left join {{ ref('dim_date') }} as dates using (date_key)
left join {{ ref('dim_model') }} as models using (model_key)
