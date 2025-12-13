{% macro float_type() -%}
  {%- if target.type in ["duckdb", "sqlite"] -%}
double
  {%- elif target.type == "bigquery" -%}
float64
  {%- else -%}
double
  {%- endif -%}
{%- endmacro %}
