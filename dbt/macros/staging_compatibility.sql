{% macro staging_cast(expression, data_type) -%}
    {%- if target.type == 'postgres' and data_type == 'timestamp' -%}
        cast({{ expression }} at time zone 'UTC' as timestamp)
    {%- elif target.type == 'postgres' -%}
        cast({{ expression }} as {{ data_type }})
    {%- else -%}
        try_cast({{ expression }} as {{ data_type }})
    {%- endif -%}
{%- endmacro %}

{% macro staging_partition_date() -%}
    {%- if target.type == 'postgres' -%}
        source_partition_date
    {%- else -%}
        try_cast(dt as date)
    {%- endif -%}
{%- endmacro %}

{% macro count_if(condition) -%}
    {%- if target.type == 'postgres' -%}
        count(*) filter (where {{ condition }})
    {%- else -%}
        count_if({{ condition }})
    {%- endif -%}
{%- endmacro %}

{% macro date_diff_days(start_date, end_date) -%}
    {%- if target.type == 'postgres' -%}
        ({{ end_date }} - {{ start_date }})
    {%- else -%}
        datediff('day', {{ start_date }}, {{ end_date }})
    {%- endif -%}
{%- endmacro %}

{% macro as_double(expression) -%}
    cast(
        {{ expression }} as
        {%- if target.type == 'postgres' %} double precision
        {%- else %} double
        {%- endif %}
    )
{%- endmacro %}

{% macro continuous_percentile(expression, percentile) -%}
    {%- if target.type == 'postgres' -%}
        percentile_cont({{ percentile }}) within group (
            order by {{ expression }}
        )
    {%- else -%}
        quantile_cont({{ expression }}, {{ percentile }})
    {%- endif -%}
{%- endmacro %}

{% macro date_diff_seconds(start_timestamp, end_timestamp) -%}
    {%- if target.type == 'postgres' -%}
        extract(epoch from (
            date_trunc('second', {{ end_timestamp }})
            - date_trunc('second', {{ start_timestamp }})
        ))
    {%- else -%}
        date_diff('second', {{ start_timestamp }}, {{ end_timestamp }})
    {%- endif -%}
{%- endmacro %}
