select
    dimension.user_key,
    dimension.user_id
from {{ ref('dim_user') }} as dimension
inner join {{ ref('current_user_state') }} as current_state
    on dimension.user_id = current_state.user_id
where dimension.signup_at is distinct from current_state.signup_at
   or dimension.signup_source is distinct from current_state.signup_source
