{% macro construct_json_object(table_name) %}

{% set parts = table_name.split('.') %}
{% set tblname = parts[-1] %}
{% set tblname_replaced = tblname | replace('\"', '') %}

{% set columns_query %}
SELECT column_name
FROM information_schema.columns
WHERE table_name = '{{ tblname_replaced }}'
{% endset %}

{% set results = run_query(columns_query) %}

{% set column_names = results.rows | map(attribute='column_name') | list %}

{% set chunk_size = 50 %}
{% set json_build_object_chunks = [] %}

{% for i in range(0, column_names | length, chunk_size) %}
    {% set chunk = column_names[i:i + chunk_size] %}
    {% set json_build_object_parts = [] %}

    {% for column_name in chunk %}
        {% set col_name = column_name %}
        {% do json_build_object_parts.append("'" ~ col_name ~ "', " ~ '"' ~ col_name ~ '"') %}
    {% endfor %}

    {% set json_build_object = json_build_object_parts | join(', ') %}
    {% do json_build_object_chunks.append("(SELECT jsonb_build_object(" ~ json_build_object ~ "))") %}
{% endfor %}

{% set combined_json_build_objects = json_build_object_chunks | join(' || ') %}

{{ log("combined_json_build_objects", info = true) }}
{{ log(combined_json_build_objects, info = true) }}

with json_objects as (
  select {{ combined_json_build_objects }} as combined_json
)
select combined_json as result
from json_objects

{% endmacro %}
