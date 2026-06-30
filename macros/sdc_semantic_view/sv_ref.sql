{#-
----------------------------------------------------------------------------------------------------------------
Description:
    This macro is a Jinja Helper to write the "base_table" in the Snowflake Semantic View yaml format 
----------------------------------------------------------------------------------------------------------------
Parameters: 
    * `ref_name` (string): name of the upstream model
    * `database_name` (string) - optional: database name to be used
    * `schema_name` (string) - optional: schema name to be used
    * `version`|`v` (string) - optional: dbt model version
----------------------------------------------------------------------------------------------------------------
Examples:
```yaml
name: MASTER_DATA
  description: Semantic view for master_data
  tables:
    - name: ifa_master_data
      description: Main table for customer data
      base_table: {{ sie_dbt_utils.sv_ref('mrt_master_data') }}

--> Will compile into:
name: MASTER_DATA
  description: Semantic view for master_data
  tables:
    - name: ifa_master_data
      description: Main table for customer data
      base_table:
        database: DEV_DATABASE  
        schema: MARTS
        table: mrt_master_data

```
----------------------------------------------------------------------------------------------------------------
-#}

{% macro sv_ref(ref_name, database_name=none, schema_name=none, v=none, version=none) %}
    {{- return(adapter.dispatch('sv_ref', 'sie_dbt_utils')(ref_name=ref_name, database_name=database_name, schema_name=schema_name, v=v, version=version)) -}}
{% endmacro %}

{% macro default__sv_ref(ref_name, database_name=none, schema_name=none,v=none, version=none) %}
{% set ref_rel=ref(ref_name, v=v if v else version) %}
{% set final_ref %}
        database: {{ database_name if database_name else ref_rel.database }}
        schema: {{ schema_name if schema_name else ref_rel.schema }}
        table: {{ ref_rel.identifier}}
{% endset %}
{{ return(final_ref) }}
{% endmacro %}


