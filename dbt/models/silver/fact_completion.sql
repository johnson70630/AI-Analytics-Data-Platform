select
    completions.completion_id,
    completions.message_id,
    completions.conversation_id,
    users.user_key,
    count(users.user_key) over (
        partition by completions.completion_id
    ) as user_key_match_count,
    dates.date_key,
    completions.user_id,
    completions.source_user_id,
    completions.completion_status,
    completions.requested_at,
    completions.completed_at,
    case
        when completions.is_timestamp_order_valid
            then extract(epoch from (
                completions.completed_at - completions.requested_at
            ))
    end as completion_duration_seconds,
    completions.ingested_at,
    completions.physical_partition_date as source_partition_date,
    completions.is_user_id_recovered,
    completions.is_user_id_missing,
    completions.is_timestamp_order_valid
from {{ ref('stg_completions') }} as completions
left join {{ ref('dim_user') }} as users
    on completions.user_id = users.user_id
   and completions.requested_at >= users.effective_start_at
   and (
       completions.requested_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_date') }} as dates
    on cast(completions.requested_at as date) = dates.full_date
