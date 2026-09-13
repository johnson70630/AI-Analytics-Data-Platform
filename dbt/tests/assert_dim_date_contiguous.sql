with date_summary as (
    select
        min(full_date) as minimum_date,
        max(full_date) as maximum_date,
        count(*) as actual_date_count
    from {{ ref('dim_date') }}
)

select
    minimum_date,
    maximum_date,
    actual_date_count
from date_summary
where actual_date_count
    != {{ date_diff_days('minimum_date', 'maximum_date') }} + 1
