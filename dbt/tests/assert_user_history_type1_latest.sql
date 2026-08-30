with ranked_type1_updates as (
    select
        update_id,
        user_id,
        field_name,
        new_value,
        row_number() over (
            partition by user_id, field_name
            order by updated_at desc, update_id desc
        ) as field_recency_rank
    from {{ ref('stg_user_updates') }}
    where field_name in ('email', 'name')
),

expected_type1 as (
    select
        users.user_id,
        case
            when email_update.update_id is not null
                then email_update.new_value
            else users.email
        end as expected_email,
        case
            when name_update.update_id is not null
                then name_update.new_value
            else users.name
        end as expected_name
    from {{ ref('stg_users') }} as users
    left join ranked_type1_updates as email_update
        on users.user_id = email_update.user_id
       and email_update.field_name = 'email'
       and email_update.field_recency_rank = 1
    left join ranked_type1_updates as name_update
        on users.user_id = name_update.user_id
       and name_update.field_name = 'name'
       and name_update.field_recency_rank = 1
)

select
    history.user_id,
    history.version_sequence,
    history.email,
    expected.expected_email,
    history.name,
    expected.expected_name
from {{ ref('user_state_history') }} as history
inner join expected_type1 as expected
    on history.user_id = expected.user_id
where history.email is distinct from expected.expected_email
   or history.name is distinct from expected.expected_name
