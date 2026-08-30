select
    errors.error_id,
    errors.conversation_id,
    errors.message_id,
    errors.completion_id,
    errors.inference_id,
    users.user_key,
    count(users.user_key) over (
        partition by errors.error_id
    ) as user_key_match_count,
    models.model_key,
    dates.date_key,
    errors.user_id,
    errors.model_id,
    errors.error_source,
    errors.error_type,
    errors.error_code,
    errors.severity,
    errors.occurred_at,
    errors.ingested_at,
    errors.physical_partition_date as source_partition_date
from {{ ref('stg_errors') }} as errors
left join {{ ref('dim_user') }} as users
    on errors.user_id = users.user_id
   and errors.occurred_at >= users.effective_start_at
   and (
       errors.occurred_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_model') }} as models
    on errors.model_id = models.model_id
left join {{ ref('dim_date') }} as dates
    on cast(errors.occurred_at as date) = dates.full_date
