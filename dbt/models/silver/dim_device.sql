select
    md5(device_id) as device_key,
    device_id,
    device_type,
    operating_system,
    browser,
    app_platform,
    physical_partition_date as source_partition_date
from {{ ref('stg_devices') }}
