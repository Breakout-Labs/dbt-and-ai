{#-
----------------------------------------------------------------------------------------------------------------
Description:
    This macro is a Jinja Helper to format long texts into yaml accepted multiline texts
----------------------------------------------------------------------------------------------------------------
Parameters: 
    * `long_text` (string): string to format
    * `indent` (int): number of whitespaces to indent the lines
----------------------------------------------------------------------------------------------------------------
Examples:
```yaml
objects:
  - name: test_Table
    description: {{ yaml_long_text(long_descr)}}
    properties:
      description: {{ yaml_long_text("This is line 1\r\nThis is line 2", 15)}}
    other_property:
        description: {{ yaml_long_text(long_descr)}}

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
----------------------------------------------------------------------------------------------------------------
-#}

{% macro yaml_long_text(long_text, indent=10) %}
        {{- return(adapter.dispatch('yaml_long_text', 'sie_dbt_utils')(long_text=long_text, indent=indent)) -}}
{% endmacro %}
{% macro default__yaml_long_text(long_text, indent=10) %}
    {% set indent_str = ' ' * indent %}
    {# simplify all carriage return into newline #}
    {% set new_text = long_text.replace('\r\n', '\n') %}
    {# indent new lines #}
    {% set new_text = long_text.replace('\n', '\n' ~ indent_str) %}
    {% if '\n' in long_text %}
        {% set new_text = '|\n' ~ indent_str ~ new_text %}
    {% endif %}
    {{ return(new_text) }}
{% endmacro %}
