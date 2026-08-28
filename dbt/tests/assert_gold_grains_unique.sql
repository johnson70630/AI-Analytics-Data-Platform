select
    'ml' as mart_name,
    cast(date_key as varchar) || '|'
        || coalesce(model_key, model_id, '__UNRESOLVED__') as grain_key
from {{ ref('gold_ml_metrics_daily') }}
group by date_key, coalesce(model_key, model_id, '__UNRESOLVED__')
having count(*) != 1

union all

select
    'user_activity',
    cast(date_key as varchar) || '|' || user_id
from {{ ref('gold_user_activity_daily') }}
group by date_key, user_id
having count(*) != 1
