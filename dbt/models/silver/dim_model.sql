select
    md5(model_id) as model_key,
    model_id,
    model_name,
    model_version,
    provider,
    release_date,
    active_flag,
    physical_partition_date as source_partition_date
from {{ ref('stg_models') }}
