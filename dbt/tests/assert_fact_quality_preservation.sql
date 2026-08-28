select 'inference_missing_model' as validation_name, inference_id as fact_id
from {{ ref('fact_model_inference') }}
where is_model_id_missing and (model_id is not null or model_key is not null)

union all

select 'completion_invalid_duration', completion_id
from {{ ref('fact_completion') }}
where is_timestamp_order_valid = false
  and completion_duration_seconds is not null

union all

select 'message_missing_text_length', message_id
from {{ ref('fact_message') }}
where is_message_text_missing and message_length_chars is not null
