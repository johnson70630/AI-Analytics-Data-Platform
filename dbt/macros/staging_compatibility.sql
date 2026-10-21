{% macro staging_cast(expression, data_type) -%}
    {%- if target.type == 'databricks' and data_type == 'varchar' -%}
        cast({{ expression }} as string)
    {%- elif target.type == 'databricks' -%}
        cast({{ expression }} as {{ data_type }})
    {%- elif data_type == 'timestamp' -%}
        cast({{ expression }} at time zone 'UTC' as timestamp)
    {%- else -%}
        cast({{ expression }} as {{ data_type }})
    {%- endif -%}
{%- endmacro %}

{% macro staging_partition_date() -%}
    {%- if target.type == 'databricks' -%}
        dt
    {%- else -%}
        source_partition_date
    {%- endif -%}
{%- endmacro %}

{% macro count_if(condition) -%}
    {%- if target.type == 'databricks' -%}
        count_if({{ condition }})
    {%- else -%}
        count(*) filter (where {{ condition }})
    {%- endif -%}
{%- endmacro %}

{% macro date_diff_days(start_date, end_date) -%}
    {%- if target.type == 'databricks' -%}
        datediff({{ end_date }}, {{ start_date }})
    {%- else -%}
        ({{ end_date }} - {{ start_date }})
    {%- endif -%}
{%- endmacro %}

{% macro as_double(expression) -%}
    {%- if target.type == 'databricks' -%}
        cast({{ expression }} as double)
    {%- else -%}
        cast({{ expression }} as double precision)
    {%- endif -%}
{%- endmacro %}

{% macro continuous_percentile(expression, percentile) -%}
    {%- if target.type == 'databricks' -%}
        percentile_cont({{ percentile }}) within group (
            order by {{ expression }}
        )
    {%- else -%}
        percentile_cont({{ percentile }}) within group (
            order by {{ expression }}
        )
    {%- endif -%}
{%- endmacro %}

{% macro date_diff_seconds(start_timestamp, end_timestamp) -%}
    {%- if target.type == 'databricks' -%}
        (
            unix_timestamp(date_trunc('second', {{ end_timestamp }}))
            - unix_timestamp(date_trunc('second', {{ start_timestamp }}))
        )
    {%- else -%}
        extract(epoch from (
            date_trunc('second', {{ end_timestamp }})
            - date_trunc('second', {{ start_timestamp }})
        ))
    {%- endif -%}
{%- endmacro %}

{% macro user_version_key(user_id, effective_start_at) -%}
    {%- if target.type == 'databricks' -%}
        md5(concat(
            cast({{ user_id }} as string),
            '|',
            cast({{ effective_start_at }} as string)
        ))
    {%- else -%}
        md5(
            {{ user_id }} || '|' || cast({{ effective_start_at }} as varchar)
        )
    {%- endif -%}
{%- endmacro %}
