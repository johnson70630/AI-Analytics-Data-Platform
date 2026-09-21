{% macro staging_cast(expression, data_type) -%}
    {%- if data_type == 'timestamp' -%}
        cast({{ expression }} at time zone 'UTC' as timestamp)
    {%- else -%}
        cast({{ expression }} as {{ data_type }})
    {%- endif -%}
{%- endmacro %}

{% macro staging_partition_date() -%}
    source_partition_date
{%- endmacro %}

{% macro count_if(condition) -%}
    count(*) filter (where {{ condition }})
{%- endmacro %}

{% macro date_diff_days(start_date, end_date) -%}
    ({{ end_date }} - {{ start_date }})
{%- endmacro %}

{% macro as_double(expression) -%}
    cast({{ expression }} as double precision)
{%- endmacro %}

{% macro continuous_percentile(expression, percentile) -%}
    percentile_cont({{ percentile }}) within group (
        order by {{ expression }}
    )
{%- endmacro %}

{% macro date_diff_seconds(start_timestamp, end_timestamp) -%}
    extract(epoch from (
        date_trunc('second', {{ end_timestamp }})
        - date_trunc('second', {{ start_timestamp }})
    ))
{%- endmacro %}
