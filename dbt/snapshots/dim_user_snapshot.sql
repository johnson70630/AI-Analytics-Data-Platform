{% snapshot dim_user_snapshot %}

select
    user_id,
    email,
    name,
    country_code,
    account_status,
    signup_at,
    signup_source,
    source_user_ingested_at,
    source_user_partition_date,
    latest_user_update_at,
    latest_update_ingested_at
from {{ ref('current_user_state') }}

{% endsnapshot %}
