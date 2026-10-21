{% if target.type == 'databricks' %}
    {{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='completion_id',
        on_schema_change='sync_all_columns'
    ) }}
{% else %}
    {{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='completion_id',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['completion_id'], 'unique': true, 'type': 'btree'}
        ]
    ) }}
{% endif %}

with completions as (
    select *
    from {{ ref('stg_completions') }}
    {% if is_incremental() %}
    where requested_at >= (
        select coalesce(
            {{ timestamp_subtract_days('max(requested_at)', 2) }},
            timestamp '1900-01-01'
        )
        from {{ this }}
    )
       or physical_partition_date >= (
           select coalesce(
               {{ date_subtract_days('max(source_partition_date)', 2) }},
               date '1900-01-01'
           )
           from {{ this }}
       )
    {% endif %}
)

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
            then {{ date_diff_seconds(
                'completions.requested_at',
                'completions.completed_at'
            ) }}
    end as completion_duration_seconds,
    completions.ingested_at,
    completions.physical_partition_date as source_partition_date,
    completions.is_user_id_recovered,
    completions.is_user_id_missing,
    completions.is_timestamp_order_valid
from completions
left join {{ ref('dim_user') }} as users
    on completions.user_id = users.user_id
   and completions.requested_at >= users.effective_start_at
   and (
       completions.requested_at < users.effective_end_at
       or users.effective_end_at is null
   )
left join {{ ref('dim_date') }} as dates
    on cast(completions.requested_at as date) = dates.full_date
