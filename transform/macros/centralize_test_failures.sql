{% macro centralize_test_failures(results) %}

    {# Only proceed if the command is 'test'. #}
    {% if flags.WHICH == 'test' and flags.STORE_FAILURES and env_var('STORE_TEST_RESULTS', 'false') == 'true'%}

      {%- set test_results = [] -%}
      {%- for result in results -%}
        {%- if result.node.resource_type == 'test' and result.status != 'skipped' and result.status != 'error' -%}
          {%- do test_results.append(result) -%}
        {%- endif -%}
      {%- endfor -%}

      {%- if test_results | length > 0 -%}
      
        {%- set central_tbl -%} {{ var('tests_schema') }}.test_failure_central {%- endset -%}
        
        {{ log("Centralizing test failures in " + central_tbl, info = true) if execute }}

        {% set adapter_type = adapter.type() %}

        {%- if adapter_type == 'snowflake' -%}
          {% set table_exists_query %}
            show tables like 'test_failure_central' in schema {{ var('tests_schema') }}
          {% endset %}
        {%- elif adapter_type == 'postgres' -%}
          {% set table_exists_query %}
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = '{{ var('tests_schema') }}'
              AND table_name = 'test_failure_central'
          {% endset %}
        {%- endif -%}


        {% set results = run_query(table_exists_query) %}
        {% if execute %}
          {% set table_exists = results and results.rows | length > 0 %}
        {% endif %}

        {% if table_exists %}
          {{ log("Table " + central_tbl + " exists, including its records.", info = true) }}
        {% else %}
          {{ log("Table " + central_tbl + " does not exist, creating table.", info = true) }}
          {% if adapter_type == 'postgres' %}
            {% set create_table %}
              create table {{ central_tbl }} (test_name varchar(255), test_run_time timestamptz, test_failures_json json)
            {% endset %}
            {% set r = run_query(create_table) %}
          {% endif %}
        {% endif %}

        {% if adapter_type == 'postgres' %}
          insert into {{ central_tbl }}(test_name, test_run_time, test_failures_json)
        {% endif %}

        {% if adapter_type == 'snowflake' %}

          create or replace table {{ central_tbl }} as
          {% if table_exists %}
            select *
            from {{ central_tbl }}
            union all
          {% endif %}

        {% endif %}
        {% for result in test_results %}

            {% set table_name = result.node.relation_name %}

            select
            '{{ result.node.unique_id }}' as test_name
            , current_timestamp as test_run_time
            {%- if adapter_type == 'snowflake' -%}
            , object_construct_keep_null(*) as test_failures_json
            from {{ result.node.relation_name }}
            {%- elif adapter_type == 'postgres' -%}
            , ({{ construct_json_object(table_name) }}) as test_failures_json
            from {{ result.node.relation_name }}
            {% endif %}

            {{ "union all" if not loop.last }}

        {% endfor %}

      {%- endif -%}

    {% else %}
      {{ log("Storing test results disabled, doing nothing.", info = true) }}
    {% endif %}

{% endmacro %}