{% macro boundary_dates() %}
  {% set today = "CURRENT_DATE" %}
  {% set year_start = "CURRENT_DATE - INTERVAL '1 year'" %}
  {% do return({'today': today, 'year_start': year_start}) %}
{% endmacro %}
