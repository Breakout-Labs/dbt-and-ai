### sv_ref ([source](macros/utilities/snowflake/sv_ref.sql))
- This macro can be used to write the "base_table" definition on a yaml Semantic View content.
- This makes it easier to adapt and create the right lineage between all the models required for a Semantic View
- This macro should be used with the `snowflake_semantic_view` custom materialization in this package (see example below)

#### Parameters.
  * `ref_name` (string): name of the upstream model
  * `database_name` (string) - optional: database name to be used
  * `schema_name` (string) - optional: schema name to be used
  * `version`|`v` (string) - optional: dbt model version

#### Syntax.
```yaml
    description: {{ sie_dbt_utils.sv_ref(ref_name, [database_name=none], [schema_name=none],[v=none | version=none]) }}
```

#### Usage.
- Use this macro within a sql file that contains yaml content, and where you want to use dbt models as base_table.


example file `models\distribute\sv_master_data.sql`:
```yaml
{{
    config(
        materialized='snowflake_semantic_view'
    )
}}

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

### yaml_long_text ([source](macros/utilities/snowflake/yaml_long_text.sql))
- This macro can be used to write long descriptions with multilines into a yaml content.
- Typically this can be used to write the `description` property of the Snowflake Semantic Views
- This macro should be used with the `snowflake_semantic_view` custom materialization in this package

#### Parameters.
  * `long_text` (string): string to format
  * `indent` (int): number of whitespaces to indent the lines

#### Syntax.
```yaml
    description: {{ sie_dbt_utils.yaml_long_text(long_text=some_long_text_variable, ident=10)}}
```

#### Usage.
- Use this macro within a sql file that contains yaml content, and where you want to include long texts as descriptions.

```
{# Define the variable with a multilined description #}
{%- set long_descr -%}
This is a multi-lines description
with a second line here
- new line starting with dash, will it show as a yaml list?
   an empty line will follow this

expected_output:
  this should be interpreted as yaml, but as a long description
{%- endset -%}

{# use the variable long_desc together with the macro #}
objects:
  - name: test_Table
    description: {{ sie_dbt_utils.yaml_long_text(long_descr)}}
    properties:
      description: {{ sie_dbt_utils.yaml_long_text("This is line 1\r\nThis is line 2", 15)}}
    other_property:
        description: {{ sie_dbt_utils.yaml_long_text(long_descr)}}

--> Will compile into:

objects:
  - name: test_Table
    description: |
          This is a multi-lines description
          with a second line here
          - new line starting with dash, will it show as a yaml list?
             an empty line will follow this
          
          expected_output:
            this should be interpreted as yaml, but as a long description
    properties:
      description: |
               This is line 1
               This is line 2
    other_property:
        description: |
          This is a multi-lines description
          with a second line here
          - new line starting with dash, will it show as a yaml list?
             an empty line will follow this
          
          expected_output:
            this should be interpreted as yaml, but as a long description
```
 
