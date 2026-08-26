with source_data as (
    select
        cast(purchase_id as varchar) as purchase_id,
        cast(user_id as varchar) as user_id,
        cast(subscription_id as varchar) as subscription_id,
        cast(plan_id as varchar) as plan_id,
        cast(purchase_type as varchar) as purchase_type,
        cast(purchase_status as varchar) as purchase_status,
        try_cast(subtotal_amount as decimal(18, 2)) as subtotal_amount,
        try_cast(discount_amount as decimal(18, 2)) as discount_amount,
        try_cast(tax_amount as decimal(18, 2)) as tax_amount,
        try_cast(total_amount as decimal(18, 2)) as total_amount,
        try_cast(purchase_created_at as timestamp) as purchase_created_at,
        try_cast(updated_at as timestamp) as updated_at,
        try_cast(ingested_at as timestamp) as ingested_at,
        try_cast(dt as date) as physical_partition_date
    from {{ source('bronze', 'purchases') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by purchase_id
            order by ingested_at asc, physical_partition_date asc
        ) as replay_rank
    from source_data
)

select
    purchase_id,
    user_id,
    subscription_id,
    plan_id,
    purchase_type,
    purchase_status,
    subtotal_amount,
    discount_amount,
    tax_amount,
    total_amount,
    purchase_created_at,
    updated_at,
    ingested_at,
    physical_partition_date,
    purchase_type is null as is_purchase_type_missing
from ranked
where replay_rank = 1
