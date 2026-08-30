with volume_metrics as (
    select
        *,
        count(*) over (
            partition by entity_name, activity_date
        ) as grain_count
    from {{ ref('dq_row_count_daily') }}
),

quality_metrics as (
    select
        *,
        count(*) over (
            partition by entity_name, metric_name, metric_date
        ) as grain_count
    from {{ ref('dq_quality_metrics_daily') }}
),

freshness_metrics as (
    select
        *,
        count(*) over (
            partition by entity_name
        ) as grain_count
    from {{ ref('dq_entity_freshness') }}
)

select
    'row_count' as model_name,
    entity_name || '|' || cast(activity_date as varchar) as grain_key
from volume_metrics
where grain_count != 1
   or row_count < 0
   or volume_status not in ('PASS', 'WARN', 'FAIL')

union all

select
    'quality_metrics',
    entity_name || '|' || metric_name || '|' || cast(metric_date as varchar)
from quality_metrics
where grain_count != 1
   or numerator < 0
   or denominator < 0
   or numerator > denominator
   or metric_rate not between 0 and 1
   or quality_status not in ('PASS', 'WARN', 'FAIL')

union all

select
    'freshness',
    entity_name
from freshness_metrics
where grain_count != 1
   or freshness_delay_hours < 0
   or freshness_status not in ('PASS', 'WARN', 'FAIL')
