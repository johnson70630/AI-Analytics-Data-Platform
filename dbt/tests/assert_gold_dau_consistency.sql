with user_daily as (
    select
        date_key,
        count_if(active_flag) as active_users
    from {{ ref('gold_user_activity_daily') }}
    group by date_key
)

select
    product.date_key,
    product.active_users as product_active_users,
    coalesce(users.active_users, 0) as user_daily_active_users
from {{ ref('gold_product_metrics_daily') }} as product
left join user_daily as users using (date_key)
where product.active_users != coalesce(users.active_users, 0)
