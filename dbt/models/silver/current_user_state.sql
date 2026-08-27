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

ranked_updates as (
    select
        update_id,
        user_id,
        field_name,
        new_value,
        updated_at,
        ingested_at,
        row_number() over (
            partition by user_id, field_name
            order by updated_at desc, update_id desc
        ) as field_recency_rank
    from {{ ref('stg_user_updates') }}
),

latest_updates as (
    select
        update_id,
        user_id,
        field_name,
        new_value,
        updated_at,
        ingested_at
    from ranked_updates
    where field_recency_rank = 1
),

update_lineage as (
    select
        user_id,
        max(updated_at) as latest_user_update_at,
        max(ingested_at) as latest_update_ingested_at
    from ranked_updates
    group by user_id
)

select
    users.user_id,
    case
        when email_update.update_id is not null then email_update.new_value
        else users.email
    end as email,
    case
        when name_update.update_id is not null then name_update.new_value
        else users.name
    end as name,
    case
        when country_update.update_id is not null then country_update.new_value
        else users.country_code
    end as country_code,
    case
        when status_update.update_id is not null then status_update.new_value
        else users.account_status
    end as account_status,
    users.signup_at,
    users.signup_source,
    users.source_user_ingested_at,
    users.source_user_partition_date,
    lineage.latest_user_update_at,
    lineage.latest_update_ingested_at
from base_users as users
left join latest_updates as email_update
    on users.user_id = email_update.user_id
   and email_update.field_name = 'email'
left join latest_updates as name_update
    on users.user_id = name_update.user_id
   and name_update.field_name = 'name'
left join latest_updates as country_update
    on users.user_id = country_update.user_id
   and country_update.field_name = 'country_code'
left join latest_updates as status_update
    on users.user_id = status_update.user_id
   and status_update.field_name = 'account_status'
left join update_lineage as lineage
    on users.user_id = lineage.user_id
