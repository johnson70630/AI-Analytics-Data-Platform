with activity_dates as (
    select date_key from {{ ref('fact_message') }}
    union all
    select date_key from {{ ref('fact_conversation') }}
    union all
    select date_key from {{ ref('fact_completion') }}
    union all
    select date_key from {{ ref('fact_feedback') }}
),

activity_bounds as (
    select
        min(date_key) as minimum_date_key,
        max(date_key) as maximum_date_key
    from activity_dates
    where date_key is not null
),

date_spine as (
    select
        dates.date_key,
        dates.full_date
    from {{ ref('dim_date') }} as dates
    cross join activity_bounds as bounds
    where dates.date_key between bounds.minimum_date_key and bounds.maximum_date_key
),

message_daily as (
    select
        date_key,
        count(*) as message_count,
        count(distinct user_id) as active_users
    from {{ ref('fact_message') }}
    group by date_key
),

conversation_daily as (
    select
        date_key,
        count(*) as conversation_count
    from {{ ref('fact_conversation') }}
    group by date_key
),

completion_daily as (
    select
        date_key,
        count(*) as completion_count,
        count_if(completion_status = 'SUCCESS') as successful_completion_count,
        count_if(completion_status = 'FAILED') as failed_completion_count,
        count_if(completion_status = 'CANCELLED') as cancelled_completion_count
    from {{ ref('fact_completion') }}
    group by date_key
),

feedback_daily as (
    select
        date_key,
        count(*) as feedback_count,
        count_if(
            feedback_type = 'THUMBS_UP'
            or (feedback_type = 'RATING' and feedback_score >= 4)
        ) as positive_feedback_count,
        count_if(
            feedback_type = 'THUMBS_DOWN'
            or (feedback_type = 'RATING' and feedback_score <= 2)
        ) as negative_feedback_count
    from {{ ref('fact_feedback') }}
    group by date_key
)

select
    dates.date_key,
    dates.full_date,
    coalesce(messages.active_users, 0) as active_users,
    coalesce(conversations.conversation_count, 0) as conversation_count,
    coalesce(messages.message_count, 0) as message_count,
    coalesce(completions.completion_count, 0) as completion_count,
    coalesce(completions.successful_completion_count, 0)
        as successful_completion_count,
    coalesce(completions.failed_completion_count, 0)
        as failed_completion_count,
    coalesce(completions.cancelled_completion_count, 0)
        as cancelled_completion_count,
    coalesce(completions.successful_completion_count, 0)::double
        / nullif(completions.completion_count, 0) as completion_success_rate,
    coalesce(feedback.feedback_count, 0) as feedback_count,
    coalesce(feedback.positive_feedback_count, 0) as positive_feedback_count,
    coalesce(feedback.negative_feedback_count, 0) as negative_feedback_count,
    coalesce(feedback.feedback_count, 0)::double
        / nullif(completions.completion_count, 0) as feedback_rate,
    coalesce(feedback.positive_feedback_count, 0)::double
        / nullif(feedback.feedback_count, 0) as positive_feedback_rate
from date_spine as dates
left join message_daily as messages using (date_key)
left join conversation_daily as conversations using (date_key)
left join completion_daily as completions using (date_key)
left join feedback_daily as feedback using (date_key)
