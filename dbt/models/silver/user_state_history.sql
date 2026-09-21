with base_users as (
    select
        user_id,
        email,
        name,
        country_code,
        account_status,
        signup_source,
        signup_at,
        ingested_at as source_user_ingested_at,
        physical_partition_date as source_user_partition_date
    from {{ ref('stg_users') }}
),

ordered_updates as (
    select
        update_id,
        user_id,
        field_name,
        old_value,
        new_value,
        updated_at,
        row_number() over (
            partition by user_id, field_name
            order by updated_at desc, update_id desc
        ) as field_recency_rank
    from {{ ref('stg_user_updates') }}
),

latest_email_updates as (
    select
        update_id,
        user_id,
        new_value
    from ordered_updates
    where field_name = 'email'
      and field_recency_rank = 1
),

latest_name_updates as (
    select
        update_id,
        user_id,
        new_value
    from ordered_updates
    where field_name = 'name'
      and field_recency_rank = 1
),

type1_state as (
    select
        users.user_id,
        case
            when email_updates.update_id is not null
                then email_updates.new_value
            else users.email
        end as email,
        case
            when name_updates.update_id is not null
                then name_updates.new_value
            else users.name
        end as name,
        users.signup_source,
        users.signup_at,
        users.source_user_ingested_at,
        users.source_user_partition_date
    from base_users as users
    left join latest_email_updates as email_updates
        on users.user_id = email_updates.user_id
    left join latest_name_updates as name_updates
        on users.user_id = name_updates.user_id
),

type2_change_events as (
    select
        update_id,
        user_id,
        field_name,
        new_value,
        updated_at
    from ordered_updates
    where field_name in ('country_code', 'account_status')
      and old_value is distinct from new_value
),

state_events as (
    select
        users.user_id,
        users.signup_at as effective_start_at,
        cast(null as varchar) as version_update_id,
        cast(null as varchar) as type2_change_field,
        users.country_code as country_code_value,
        users.account_status as account_status_value
    from base_users as users

    union all

    select
        updates.user_id,
        updates.updated_at as effective_start_at,
        updates.update_id as version_update_id,
        updates.field_name as type2_change_field,
        case
            when updates.field_name = 'country_code' then updates.new_value
        end as country_code_value,
        case
            when updates.field_name = 'account_status' then updates.new_value
        end as account_status_value
    from type2_change_events as updates
),

reconstructed_states as (
    select
        events.user_id,
        events.effective_start_at,
        events.version_update_id,
        events.type2_change_field,
        (
            select prior.country_code_value
            from state_events as prior
            where prior.user_id = events.user_id
              and prior.country_code_value is not null
              and (
                  prior.effective_start_at < events.effective_start_at
                  or (
                      prior.effective_start_at = events.effective_start_at
                      and coalesce(prior.version_update_id, '')
                          <= coalesce(events.version_update_id, '')
                  )
              )
            order by
                prior.effective_start_at desc,
                coalesce(prior.version_update_id, '') desc
            limit 1
        ) as country_code,
        (
            select prior.account_status_value
            from state_events as prior
            where prior.user_id = events.user_id
              and prior.account_status_value is not null
              and (
                  prior.effective_start_at < events.effective_start_at
                  or (
                      prior.effective_start_at = events.effective_start_at
                      and coalesce(prior.version_update_id, '')
                          <= coalesce(events.version_update_id, '')
                  )
              )
            order by
                prior.effective_start_at desc,
                coalesce(prior.version_update_id, '') desc
            limit 1
        ) as account_status
    from state_events as events
),

versioned_states as (
    select
        *,
        row_number() over (
            partition by user_id
            order by effective_start_at, coalesce(version_update_id, '')
        ) as version_sequence,
        lead(effective_start_at) over (
            partition by user_id
            order by effective_start_at, coalesce(version_update_id, '')
        ) as effective_end_at
    from reconstructed_states
)

select
    states.user_id,
    states.version_sequence,
    type1.email,
    type1.name,
    states.country_code,
    states.account_status,
    type1.signup_source,
    type1.signup_at,
    type1.source_user_ingested_at,
    type1.source_user_partition_date,
    states.version_update_id,
    states.type2_change_field,
    states.effective_start_at,
    states.effective_end_at,
    states.effective_end_at is null as is_current
from versioned_states as states
inner join type1_state as type1
    on states.user_id = type1.user_id
