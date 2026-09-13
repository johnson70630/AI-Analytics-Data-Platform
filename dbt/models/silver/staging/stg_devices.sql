with source_data as (
    select
        cast(device_id as varchar) as device_id,
        cast(device_type as varchar) as device_type,
        cast(operating_system as varchar) as operating_system,
        cast(browser as varchar) as browser,
        cast(app_platform as varchar) as app_platform,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'devices') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by device_id
            order by physical_partition_date desc nulls last
        ) as snapshot_rank
    from source_data
)

select
    device_id,
    device_type,
    operating_system,
    browser,
    app_platform,
    physical_partition_date
from ranked
where snapshot_rank = 1
