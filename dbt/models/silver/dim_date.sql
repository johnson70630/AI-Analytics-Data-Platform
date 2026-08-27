with relevant_dates as (
    select release_date as calendar_date
    from {{ ref('stg_models') }}

    union all

    select cast(
        unnest([
            signup_at,
            cast(source_user_partition_date as timestamp),
            latest_user_update_at
        ]) as date
    )
    from {{ ref('current_user_state') }}

    union all

    select cast(unnest([started_at, ended_at, updated_at]) as date)
    from {{ ref('stg_subscriptions') }}

    union all

    select cast(unnest([dbt_valid_from, dbt_valid_to]) as date)
    from {{ ref('dim_user_snapshot') }}
),

date_bounds as (
    select
        min(calendar_date) - interval 1 day as minimum_date,
        max(calendar_date) + interval 1 day as maximum_date
    from relevant_dates
    where calendar_date is not null
),

calendar as (
    select
        unnest(
            generate_series(minimum_date, maximum_date, interval 1 day)
        )::date as full_date
    from date_bounds
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
    full_date,
    cast(strftime(full_date, '%u') as integer) as day_of_week,
    day(full_date) as day_of_month,
    weekofyear(full_date) as week_of_year,
    month(full_date) as month_number,
    strftime(full_date, '%B') as month_name,
    quarter(full_date) as quarter,
    year(full_date) as year,
    cast(strftime(full_date, '%u') as integer) in (6, 7) as is_weekend
from calendar
