{% if target.type == 'databricks' %}
with relevant_dates as (
    select release_date as calendar_date
    from {{ ref('stg_models') }}

    union all

    select explode(array(
        cast(signup_at as date),
        cast(source_user_partition_date as date),
        cast(latest_user_update_at as date)
    )) as calendar_date
    from {{ ref('current_user_state') }}

    union all

    select explode(array(
        cast(started_at as date),
        cast(ended_at as date),
        cast(updated_at as date)
    )) as calendar_date
    from {{ ref('stg_subscriptions') }}

    union all

    select explode(array(
        cast(dbt_valid_from as date),
        cast(dbt_valid_to as date)
    )) as calendar_date
    from {{ ref('dim_user_snapshot') }}
),

date_bounds as (
    select
        date_sub(min(calendar_date), 1) as minimum_date,
        date_add(max(calendar_date), 1) as maximum_date
    from relevant_dates
    where calendar_date is not null
),

calendar as (
    select explode(sequence(
        minimum_date,
        maximum_date,
        interval 1 day
    )) as full_date
    from date_bounds
)

select
    cast(date_format(full_date, 'yyyyMMdd') as integer) as date_key,
    full_date,
    weekday(full_date) + 1 as day_of_week,
    dayofmonth(full_date) as day_of_month,
    weekofyear(full_date) as week_of_year,
    month(full_date) as month_number,
    date_format(full_date, 'MMMM') as month_name,
    quarter(full_date) as quarter,
    year(full_date) as year,
    weekday(full_date) + 1 in (6, 7) as is_weekend
from calendar
{% else %}
with relevant_dates as (
    select release_date as calendar_date
    from {{ ref('stg_models') }}

    union all

    select cast(dates.calendar_date as date)
    from {{ ref('current_user_state') }}
    cross join lateral (
        values
            (signup_at),
            (cast(source_user_partition_date as timestamp)),
            (latest_user_update_at)
    ) as dates(calendar_date)

    union all

    select cast(dates.calendar_date as date)
    from {{ ref('stg_subscriptions') }}
    cross join lateral (
        values (started_at), (ended_at), (updated_at)
    ) as dates(calendar_date)

    union all

    select cast(dates.calendar_date as date)
    from {{ ref('dim_user_snapshot') }}
    cross join lateral (
        values (dbt_valid_from), (dbt_valid_to)
    ) as dates(calendar_date)
),

date_bounds as (
    select
        min(calendar_date) - interval '1 day' as minimum_date,
        max(calendar_date) + interval '1 day' as maximum_date
    from relevant_dates
    where calendar_date is not null
),

calendar as (
    select generated.full_date::date as full_date
    from date_bounds
    cross join lateral generate_series(
        minimum_date,
        maximum_date,
        interval '1 day'
    ) as generated(full_date)
)

select
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
    full_date,
    cast(to_char(full_date, 'ID') as integer) as day_of_week,
    extract(day from full_date)::integer as day_of_month,
    extract(week from full_date)::integer as week_of_year,
    extract(month from full_date)::integer as month_number,
    to_char(full_date, 'FMMonth') as month_name,
    extract(quarter from full_date)::integer as quarter,
    extract(year from full_date)::integer as year,
    cast(to_char(full_date, 'ID') as integer) in (6, 7) as is_weekend
from calendar
{% endif %}
