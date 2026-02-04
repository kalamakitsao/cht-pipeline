{{ config(
    materialized = 'incremental',
    unique_key = 'chp_area_id',
    on_schema_change = 'append_new_columns',
    indexes = [
        {'columns': ['chp_area_id'], 'unique': true},
        {'columns': ['county_id']},
        {'columns': ['sub_county_id']},
        {'columns': ['community_unit_id']},
        {'columns': ['chp_area']}
    ],
    tags=['cadence_hourly']
) }}

WITH base AS (
    SELECT
        county.location_id AS county_id,
        county.name AS county,
        sub.location_id AS sub_county_id,
        sub.name AS sub_county,
        chu.location_id AS community_unit_id,
        chu.name AS community_unit,
        chp_area.location_id AS chp_area_id,
        chp_area.name AS chp_area
    FROM {{ ref('dim_location') }} chp_area
    JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'chp_hierarchy') }} ch ON chp_area.location_id = ch.uuid
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
