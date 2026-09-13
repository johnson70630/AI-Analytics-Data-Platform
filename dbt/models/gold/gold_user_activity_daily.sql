with message_user_daily as (
    select
        user_id,
        date_key,
        count(*) as message_count
    from {{ ref('fact_message') }}
    where user_id is not null and date_key is not null
    group by user_id, date_key
),

conversation_user_daily as (
    select
        user_id,
        date_key,
        count(*) as conversation_count
    from {{ ref('fact_conversation') }}
    where user_id is not null and date_key is not null
    group by user_id, date_key
),

completion_user_daily as (
    select
        user_id,
        date_key,
        count(*) as completion_count,
        {{ count_if("completion_status = 'SUCCESS'") }}
            as successful_completion_count
    from {{ ref('fact_completion') }}
    where user_id is not null and date_key is not null
    group by user_id, date_key
),

feedback_user_daily as (
    select
        user_id,
        date_key,
        count(*) as feedback_count,
        {{ count_if(
            "feedback_type = 'THUMBS_UP' "
            ~ "or (feedback_type = 'RATING' and feedback_score >= 4)"
        ) }} as positive_feedback_count
    from {{ ref('fact_feedback') }}
    where user_id is not null and date_key is not null
    group by user_id, date_key
),

inference_user_daily as (
    select
        user_id,
        date_key,
        count(*) as inference_count,
        sum(input_tokens) as input_tokens,
        sum(output_tokens) as output_tokens,
        sum(total_tokens) as total_tokens
    from {{ ref('fact_model_inference') }}
    where user_id is not null and date_key is not null
    group by user_id, date_key
),

activity_keys as (
    select user_id, date_key from message_user_daily
    union
    select user_id, date_key from conversation_user_daily
    union
    select user_id, date_key from completion_user_daily
    union
    select user_id, date_key from feedback_user_daily
    union
    select user_id, date_key from inference_user_daily
)

select
    activity.date_key,
    dates.full_date,
    activity.user_id,
    coalesce(messages.message_count, 0) > 0 as active_flag,
    coalesce(messages.message_count, 0) as message_count,
    coalesce(conversations.conversation_count, 0) as conversation_count,
    coalesce(completions.completion_count, 0) as completion_count,
    coalesce(completions.successful_completion_count, 0)
        as successful_completion_count,
    coalesce(feedback.feedback_count, 0) as feedback_count,
    coalesce(feedback.positive_feedback_count, 0) as positive_feedback_count,
    coalesce(inferences.inference_count, 0) as inference_count,
    coalesce(inferences.input_tokens, 0) as input_tokens,
    coalesce(inferences.output_tokens, 0) as output_tokens,
    coalesce(inferences.total_tokens, 0) as total_tokens
from activity_keys as activity
inner join {{ ref('dim_date') }} as dates using (date_key)
left join message_user_daily as messages using (user_id, date_key)
left join conversation_user_daily as conversations using (user_id, date_key)
left join completion_user_daily as completions using (user_id, date_key)
left join feedback_user_daily as feedback using (user_id, date_key)
left join inference_user_daily as inferences using (user_id, date_key)
