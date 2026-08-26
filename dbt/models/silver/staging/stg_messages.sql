with source_data as (
    select
        cast(message_id as varchar) as message_id,
        cast(conversation_id as varchar) as conversation_id,
        cast(user_id as varchar) as user_id,
        cast(device_id as varchar) as device_id,
        try_cast(sequence_number as bigint) as sequence_number,
        cast(message_text as varchar) as message_text,
        try_cast(created_at as timestamp) as created_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'messages') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by message_id
            order by ingested_at asc, physical_partition_date asc
        ) as replay_rank
    from source_data
),

deduplicated as (
    select
        message_id,
        conversation_id,
        user_id,
        device_id,
        sequence_number,
        message_text,
        created_at,
        ingested_at,
        physical_partition_date
    from ranked
    where replay_rank = 1
)

select
    messages.message_id,
    messages.conversation_id,
    messages.user_id,
    messages.device_id,
    messages.sequence_number,
    messages.message_text,
    messages.created_at,
    messages.ingested_at,
    messages.physical_partition_date,
    messages.message_text is null as is_message_text_missing,
    case
        when conversations.conversation_id is null then null
        else messages.created_at >= conversations.created_at
    end as is_timestamp_order_valid
from deduplicated as messages
left join {{ ref('stg_conversations') }} as conversations
    on messages.conversation_id = conversations.conversation_id
