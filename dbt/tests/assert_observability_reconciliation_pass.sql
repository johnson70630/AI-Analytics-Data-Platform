select
    reconciliation_name,
    source_count,
    target_count,
    difference,
    status
from {{ ref('dq_pipeline_reconciliation') }}
where difference != 0
   or status != 'PASS'
