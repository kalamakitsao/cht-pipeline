{% macro place_period() %}
place_period AS (
  SELECT
    county_name,
    sub_county_name,
    chu_code,
    chu_name,
    chp_area_uuid,
    chp_area_name,
    phone,
    period_CTE.period_start AS period_start
  FROM
    {{ ref('chp_hierarchy') }},
    period_CTE
),
{% endmacro %}