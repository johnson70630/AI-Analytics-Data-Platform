with source_data as (
    select
        {{ staging_cast('device_id', 'varchar') }} as device_id,
        {{ staging_cast('device_type', 'varchar') }} as device_type,
        {{ staging_cast('operating_system', 'varchar') }} as operating_system,
        {{ staging_cast('browser', 'varchar') }} as browser,
        {{ staging_cast('app_platform', 'varchar') }} as app_platform,
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
