select
    completion_id,
    completion_duration_seconds
from {{ ref('fact_completion') }}
where completion_duration_seconds < 0
