{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='message_id',
    on_schema_change='sync_all_columns',
    indexes=[
        {'columns': ['message_id'], 'unique': true, 'type': 'btree'}
    ]
) }}

with messages as (
    select *
    from {{ ref('stg_messages') }}
    {% if is_incremental() %}
    where created_at >= (
        select coalesce(
            max(created_at) - interval '2 days',
            timestamp '1900-01-01'
        )
        from {{ this }}
    )
       or physical_partition_date >= (
           select coalesce(
               max(source_partition_date) - 2,
               date '1900-01-01'
           )
           from {{ this }}
       )
    {% endif %}
)

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
from messages
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
