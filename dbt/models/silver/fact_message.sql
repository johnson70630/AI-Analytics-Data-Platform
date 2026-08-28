select
    messages.message_id,
    messages.conversation_id,
    users.user_key,
    count(users.user_key) over (
        partition by messages.message_id
    ) as user_key_match_count,
    devices.device_key,
    dates.date_key,
    messages.user_id,
    messages.device_id,
    messages.sequence_number,
    case
        when messages.message_text is null then null
        else length(messages.message_text)
    end as message_length_chars,
    messages.created_at,
    messages.ingested_at,
    messages.physical_partition_date as source_partition_date,
    messages.is_message_text_missing,
    messages.is_timestamp_order_valid
from {{ ref('stg_messages') }} as messages
left join {{ ref('dim_user') }} as users
    on messages.user_id = users.user_id
   and messages.created_at >= users.effective_start_at
   and (
       messages.created_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_device') }} as devices
    on messages.device_id = devices.device_id
left join {{ ref('dim_date') }} as dates
    on cast(messages.created_at as date) = dates.full_date
