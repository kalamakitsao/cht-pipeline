{% macro users_place() %}
  WITH users_place AS (
    SELECT
      split_part(us.user_id, ':', 2) AS user_name,
      us.contact_uuid AS chp_id,
      chp.county_name,
      chp.sub_county_name,
      chp.chu_name
    FROM
      {{ ref('user') }} AS us
    JOIN 
      {{ ref('chp_hierarchy') }} AS chp ON us.contact_uuid = chp.chp_id
  )
{% endmacro %}