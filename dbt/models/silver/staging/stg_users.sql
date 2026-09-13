with source_data as (
    select
        cast(user_id as varchar) as user_id,
        cast(email as varchar) as email,
        cast(name as varchar) as name,
        cast(country_code as varchar) as country_code,
        cast(account_status as varchar) as account_status,
        cast(signup_source as varchar) as signup_source,
        {{ staging_cast('signup_at', 'timestamp') }} as signup_at,
        {{ staging_cast('ingested_at', 'timestamp') }} as ingested_at,
        {{ staging_partition_date() }} as physical_partition_date
    from {{ source('bronze', 'users') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by user_id
            order by
                ingested_at asc nulls last,
                physical_partition_date asc nulls last
        ) as replay_rank
    from source_data
)

select
    user_id,
    email,
    name,
    country_code,
    account_status,
    signup_source,
    signup_at,
    ingested_at,
    physical_partition_date,
    email is null as is_email_missing
from ranked
where replay_rank = 1
