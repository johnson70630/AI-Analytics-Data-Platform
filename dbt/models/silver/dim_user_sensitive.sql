{{ config(
    materialized='table',
    grants={
        'select': ['pii_approved']
    },
    post_hook='revoke all on {{ this }} from public, analytics_reader'
) }}

select
    users.user_key,
    users.user_id,
    current_state.email,
    current_state.name
from {{ ref('dim_user') }} as users
inner join {{ ref('current_user_state') }} as current_state
    on users.user_id = current_state.user_id
