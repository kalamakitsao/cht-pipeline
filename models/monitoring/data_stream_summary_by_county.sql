{{ config(
    materialized = 'incremental',
    unique_key = 'county',
    on_schema_change = 'append_new_columns'
) }}

WITH location_hierarchy AS (
    SELECT
        chp_area.location_id AS chp_area_id,
        county.name AS county
    FROM {{ ref('dim_location') }} chp_area
    LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
    LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
    LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
    WHERE chp_area.level = 'chp area'
      AND chp_area.name !~ '^[0-9]+$'
),

contacts_with_location AS (
    SELECT
        lh.county,
        c.reported
    FROM {{ ref('contact') }} c
    JOIN location_hierarchy lh ON lh.chp_area_id = c.parent_uuid
),

reports_with_location AS (
    SELECT
        lh.county,
        d.reported
    FROM {{ ref('data_record') }} d
    JOIN location_hierarchy lh ON lh.chp_area_id = d.grandparent_uuid
    WHERE d.reported >= CURRENT_DATE - INTERVAL '3 days'
),

aggregated AS (
    SELECT
        COALESCE(c.county, r.county) AS county,
        COUNT(c.reported) AS contact_count,
        COUNT(r.reported) AS report_count,
        MIN(r.reported) AS earliest_report,
        MAX(r.reported) AS latest_report
    FROM contacts_with_location c
    FULL OUTER JOIN reports_with_location r ON c.county = r.county
    GROUP BY COALESCE(c.county, r.county)
)

SELECT
    county,
    contact_count,
    report_count,
    earliest_report,
    latest_report
FROM aggregated
