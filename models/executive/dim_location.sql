{{ config(
    materialized='table',
    tags=["dimension", "location"],
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_location_location_id ON {{ this }} (location_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_location_level ON {{ this }} (level)",
        "CREATE INDEX IF NOT EXISTS idx_dim_location_parent_id ON {{ this }} (parent_id)"
    ]
) }}

SELECT
    uuid AS location_id,
    name,
    CASE contact_type
        WHEN 'a_county' THEN 'county'
        WHEN 'b_sub_county' THEN 'sub county'
        WHEN 'c_community_health_unit' THEN 'community unit'
        WHEN 'd_community_health_volunteer_area' THEN 'chp area'
    END AS level,
    parent_uuid AS parent_id
FROM {{ ref('contact') }}
WHERE contact_type IN (
    'a_county',
    'b_sub_county',
    'c_community_health_unit',
    'd_community_health_volunteer_area'
)
