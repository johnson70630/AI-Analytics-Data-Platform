with source_data as (
    select
        cast(model_id as varchar) as model_id,
        cast(model_name as varchar) as model_name,
        cast(model_version as varchar) as model_version,
        cast(provider as varchar) as provider,
        try_cast(release_date as date) as release_date,
        try_cast(active_flag as boolean) as active_flag,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'models') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by model_id
            order by physical_partition_date desc
        ) as snapshot_rank
    from source_data
)

select
    model_id,
    model_name,
    model_version,
    provider,
    release_date,
    active_flag,
    physical_partition_date
from ranked
where snapshot_rank = 1
