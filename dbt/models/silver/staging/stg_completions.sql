with source_data as (
    select
        cast(completion_id as varchar) as completion_id,
        cast(message_id as varchar) as message_id,
        cast(conversation_id as varchar) as conversation_id,
        cast(user_id as varchar) as source_user_id,
        cast(completion_status as varchar) as completion_status,
        {{ staging_cast('requested_at', 'timestamp') }} as requested_at,
        {{ staging_cast('completed_at', 'timestamp') }} as completed_at,
        cast(response_text as varchar) as response_text,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'completions') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by completion_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
),

deduplicated as (
    select
        completion_id,
        message_id,
        conversation_id,
        source_user_id,
        completion_status,
        requested_at,
        completed_at,
        response_text,
        ingested_at,
        physical_partition_date
    from ranked
    where replay_rank = 1
)

select
    completions.completion_id,
    completions.message_id,
    completions.conversation_id,
    completions.source_user_id,
    coalesce(completions.source_user_id, messages.user_id) as user_id,
    completions.completion_status,
    completions.requested_at,
    completions.completed_at,
    completions.response_text,
    completions.ingested_at,
    completions.physical_partition_date,
    completions.source_user_id is null
        and messages.user_id is not null as is_user_id_recovered,
    coalesce(completions.source_user_id, messages.user_id) is null
        as is_user_id_missing,
    case
        when completions.completed_at is null then null
        else completions.completed_at >= completions.requested_at
    end as is_timestamp_order_valid
from deduplicated as completions
left join {{ ref('stg_messages') }} as messages
    on completions.message_id = messages.message_id
