select
    conversations.conversation_id,
    users.user_key,
    count(users.user_key) over (
        partition by conversations.conversation_id
    ) as user_key_match_count,
    dates.date_key,
    conversations.user_id,
    conversations.created_at,
    conversations.ingested_at,
    conversations.physical_partition_date as source_partition_date
from {{ ref('stg_conversations') }} as conversations
left join {{ ref('dim_user') }} as users
    on conversations.user_id = users.user_id
   and conversations.created_at >= users.effective_start_at
   and (
       conversations.created_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_date') }} as dates
    on cast(conversations.created_at as date) = dates.full_date
