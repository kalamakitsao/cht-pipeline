{{ config(
    materialized = 'incremental',
    unique_key = 'chp_area_id',
    on_schema_change = 'ignore',
    indexes = [
        {'columns': ['chp_area_id'], 'unique': true},
        {'columns': ['county']},
        {'columns': ['sub_county']},
        {'columns': ['community_unit']},
        {'columns': ['chp_area']}
    ]
) }}

WITH base AS (
    SELECT
        county.name AS county,
        sub.name AS sub_county,
        chu.name AS community_unit,
        chp_area.name AS chp_area,
        chp_area.location_id AS chp_area_id
    FROM {{ ref('dim_location') }} chp_area
    LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
    LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
    LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
    WHERE chp_area.level = 'chp area'
      AND chp_area.name !~ '^[0-9]+$'
      AND county.name IS NOT NULL
      AND county.level = 'county'
)

SELECT * FROM base

{% if is_incremental() %}
WHERE chp_area_id NOT IN (SELECT chp_area_id FROM {{ this }})
{% endif %}
