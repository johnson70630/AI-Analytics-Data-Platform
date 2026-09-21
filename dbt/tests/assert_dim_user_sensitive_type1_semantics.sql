select
    sensitive.user_key,
    sensitive.user_id
from {{ ref('dim_user_sensitive') }} as sensitive
inner join {{ ref('current_user_state') }} as current_state
    on sensitive.user_id = current_state.user_id
where sensitive.email is distinct from current_state.email
   or sensitive.name is distinct from current_state.name
