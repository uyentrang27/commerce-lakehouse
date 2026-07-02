{# Use the custom schema name verbatim (staging, gold, snapshots) instead of
   dbt's default <target>_<custom> prefixing — keeps warehouse schemas clean. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{# Read a Silver Parquet dataset from the lake by name. SILVER_PATH is set by
   the orchestrator / Makefile so dbt and Spark agree on the lake location. #}
{% macro silver(name, glob='*.parquet', opts='') -%}
    read_parquet('{{ env_var('SILVER_PATH') }}/{{ name }}/{{ glob }}'{{ opts }})
{%- endmacro %}
