with subscription_start_daily as (
    select
        date_key,
        count(*) as subscription_start_count
    from {{ ref('fact_subscription') }}
    group by date_key
),

subscription_end_daily as (
    select
        dates.date_key,
        count(*) as subscription_end_count,
        count_if(subscriptions.subscription_status = 'CANCELLED')
            as cancelled_subscription_count,
        count_if(subscriptions.subscription_status = 'EXPIRED')
            as expired_subscription_count
    from {{ ref('fact_subscription') }} as subscriptions
    inner join {{ ref('dim_date') }} as dates
        on cast(subscriptions.ended_at as date) = dates.full_date
    where subscriptions.ended_at is not null
    group by dates.date_key
),

purchase_daily as (
    select
        date_key,
        count(*) as purchase_count,
        count_if(purchase_status = 'COMPLETED') as completed_purchase_count,
        count_if(purchase_status = 'FAILED') as failed_purchase_count,
        count_if(purchase_status = 'REFUNDED') as refunded_purchase_count,
        count_if(purchase_type = 'NEW_SUBSCRIPTION')
            as new_subscription_purchase_count,
        count_if(purchase_type = 'RENEWAL') as renewal_purchase_count,
        count_if(purchase_type = 'UPGRADE') as upgrade_purchase_count,
        sum(subtotal_amount) filter (where purchase_status = 'COMPLETED')
            as completed_subtotal_amount,
        sum(discount_amount) filter (where purchase_status = 'COMPLETED')
            as completed_discount_amount,
        sum(tax_amount) filter (where purchase_status = 'COMPLETED')
            as completed_tax_amount,
        sum(total_amount) filter (where purchase_status = 'COMPLETED')
            as completed_purchase_amount
    from {{ ref('fact_purchase') }}
    group by date_key
),

payment_daily as (
    select
        date_key,
        count(*) as payment_attempt_count,
        count_if(payment_status = 'SUCCESS') as successful_payment_count,
        count_if(payment_status = 'FAILED') as failed_payment_count,
        count_if(payment_status = 'REFUNDED') as refunded_payment_count,
        sum(
            case
                when payment_status in ('SUCCESS', 'REFUNDED')
                    then payment_amount
                else 0
            end
        ) as gross_collected_amount,
        sum(refund_amount) as refund_amount
    from {{ ref('fact_payment') }}
    group by date_key
),

payment_attempts_per_purchase as (
    select
        purchase_id,
        min(date_key) as first_payment_date_key,
        count(*) as attempt_count
    from {{ ref('fact_payment') }}
    group by purchase_id
),

retry_daily as (
    select
        first_payment_date_key as date_key,
        count(*) as purchases_with_payment_attempts,
        count_if(attempt_count > 1) as purchases_with_multiple_payment_attempts,
        sum(greatest(attempt_count - 1, 0)) as retry_attempt_count
    from payment_attempts_per_purchase
    group by first_payment_date_key
),

activity_dates as (
    select date_key from subscription_start_daily
    union all
    select date_key from subscription_end_daily
    union all
    select date_key from purchase_daily
    union all
    select date_key from payment_daily
),

activity_bounds as (
    select
        min(date_key) as minimum_date_key,
        max(date_key) as maximum_date_key
    from activity_dates
    where date_key is not null
),

date_spine as (
    select
        dates.date_key,
        dates.full_date
    from {{ ref('dim_date') }} as dates
    cross join activity_bounds as bounds
    where dates.date_key between bounds.minimum_date_key and bounds.maximum_date_key
)

select
    dates.date_key,
    dates.full_date,
    coalesce(starts.subscription_start_count, 0) as subscription_start_count,
    coalesce(ends.subscription_end_count, 0) as subscription_end_count,
    coalesce(ends.cancelled_subscription_count, 0)
        as cancelled_subscription_count,
    coalesce(ends.expired_subscription_count, 0) as expired_subscription_count,
    coalesce(purchases.purchase_count, 0) as purchase_count,
    coalesce(purchases.completed_purchase_count, 0) as completed_purchase_count,
    coalesce(purchases.failed_purchase_count, 0) as failed_purchase_count,
    coalesce(purchases.refunded_purchase_count, 0) as refunded_purchase_count,
    coalesce(purchases.new_subscription_purchase_count, 0)
        as new_subscription_purchase_count,
    coalesce(purchases.renewal_purchase_count, 0) as renewal_purchase_count,
    coalesce(purchases.upgrade_purchase_count, 0) as upgrade_purchase_count,
    coalesce(purchases.completed_subtotal_amount, 0)
        as completed_subtotal_amount,
    coalesce(purchases.completed_discount_amount, 0)
        as completed_discount_amount,
    coalesce(purchases.completed_tax_amount, 0) as completed_tax_amount,
    coalesce(purchases.completed_purchase_amount, 0) as completed_purchase_amount,
    coalesce(payments.payment_attempt_count, 0) as payment_attempt_count,
    coalesce(payments.successful_payment_count, 0) as successful_payment_count,
    coalesce(payments.failed_payment_count, 0) as failed_payment_count,
    coalesce(payments.refunded_payment_count, 0) as refunded_payment_count,
    coalesce(payments.successful_payment_count, 0)::double
        / nullif(payments.payment_attempt_count, 0) as payment_success_rate,
    coalesce(payments.gross_collected_amount, 0) as gross_collected_amount,
    coalesce(payments.refund_amount, 0) as refund_amount,
    coalesce(payments.gross_collected_amount, 0)
        - coalesce(payments.refund_amount, 0) as net_revenue,
    coalesce(retries.purchases_with_payment_attempts, 0)
        as purchases_with_payment_attempts,
    coalesce(retries.purchases_with_multiple_payment_attempts, 0)
        as purchases_with_multiple_payment_attempts,
    coalesce(retries.retry_attempt_count, 0) as retry_attempt_count,
    coalesce(retries.purchases_with_multiple_payment_attempts, 0)::double
        / nullif(retries.purchases_with_payment_attempts, 0)
        as retry_purchase_rate
from date_spine as dates
left join subscription_start_daily as starts using (date_key)
left join subscription_end_daily as ends using (date_key)
left join purchase_daily as purchases using (date_key)
left join payment_daily as payments using (date_key)
left join retry_daily as retries using (date_key)
