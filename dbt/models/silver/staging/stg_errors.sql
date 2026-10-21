with source_data as (
    select
        {{ staging_cast('error_id', 'varchar') }} as error_id,
        {{ staging_cast('user_id', 'varchar') }} as user_id,
        {{ staging_cast('conversation_id', 'varchar') }} as conversation_id,
        {{ staging_cast('message_id', 'varchar') }} as message_id,
        {{ staging_cast('completion_id', 'varchar') }} as completion_id,
        {{ staging_cast('inference_id', 'varchar') }} as inference_id,
        {{ staging_cast('model_id', 'varchar') }} as model_id,
        {{ staging_cast('error_source', 'varchar') }} as error_source,
        {{ staging_cast('error_type', 'varchar') }} as error_type,
        {{ staging_cast('error_code', 'varchar') }} as error_code,
        {{ staging_cast('severity', 'varchar') }} as severity,
        {{ staging_cast('occurred_at', 'timestamp') }} as occurred_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'errors') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by error_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    error_id,
    user_id,
    conversation_id,
    message_id,
    completion_id,
    inference_id,
    model_id,
    error_source,
    error_type,
    error_code,
    severity,
    occurred_at,
    ingested_at,
    physical_partition_date
from ranked
where replay_rank = 1
