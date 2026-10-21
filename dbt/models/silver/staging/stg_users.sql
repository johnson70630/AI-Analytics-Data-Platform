with source_data as (
    select
        {{ staging_cast('user_id', 'varchar') }} as user_id,
        {{ staging_cast('email', 'varchar') }} as email,
        {{ staging_cast('name', 'varchar') }} as name,
        {{ staging_cast('country_code', 'varchar') }} as country_code,
        {{ staging_cast('account_status', 'varchar') }} as account_status,
        {{ staging_cast('signup_source', 'varchar') }} as signup_source,
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
