{%- materialization snowflake_semantic_view, default -%}
  {%- set target_relation = api.Relation.create(
    schema=schema,
    database=database
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {% call statement() -%}
    use {{ model.database }}.{{ model.schema }}
  {%- endcall %}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# parse the yaml code(in sql variable) to avoid errors #}
  {% set parsed_sql=sql %}
  {% if (sql | trim | lower).startswith('name:') %}
    {% set se=modules.re.search('(\s*name:\s*[^\\n]+)(.*)', parsed_sql, flags=modules.re.IGNORECASE + modules.re.DOTALL) %}
    {{ log('model `'~ model.name ~'` materialized with snowflake_semantic_view starts with "name:", this line will be ignored: '~se.group(1), info=True) }}
    {% set parsed_sql=se.group(2) %}
  {% endif %}
  {% set parsed_sql_full %}
name: {{model.get('alias')|upper}}
{{ parsed_sql}}
  {% endset %}

  {% call statement('main') -%}
    CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('{{model.database|upper}}.{{model.schema|upper}}',$${{ parsed_sql_full }}$$)
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
 