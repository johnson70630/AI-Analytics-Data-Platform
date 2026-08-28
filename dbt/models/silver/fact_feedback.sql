select
    feedback.feedback_id,
    feedback.completion_id,
    users.user_key,
    count(users.user_key) over (
        partition by feedback.feedback_id
    ) as user_key_match_count,
    dates.date_key,
    feedback.user_id,
    feedback.feedback_type,
    feedback.feedback_score,
    feedback.created_at,
    feedback.ingested_at,
    feedback.physical_partition_date as source_partition_date,
    feedback.is_feedback_type_missing
from {{ ref('stg_feedback') }} as feedback
left join {{ ref('dim_user') }} as users
    on feedback.user_id = users.user_id
   and feedback.created_at >= users.effective_start_at
   and (
       feedback.created_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_date') }} as dates
    on cast(feedback.created_at as date) = dates.full_date
