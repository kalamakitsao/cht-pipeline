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

agg_contacts AS (
    SELECT
        lh.county,
        COUNT(*) AS contact_count
    FROM {{ ref('contact') }} c
    JOIN location_hierarchy lh ON lh.chp_area_id = c.parent_uuid
    GROUP BY lh.county
),

agg_reports AS (
    SELECT
        lh.county,
        COUNT(*) AS report_count,
        MIN(d.reported) AS earliest_report,
        MAX(d.reported) AS latest_report
    FROM {{ ref('data_record') }} d
    JOIN location_hierarchy lh ON lh.chp_area_id = d.parent_uuid
    WHERE d.reported >= CURRENT_DATE - INTERVAL '3 days'
    GROUP BY lh.county
)
SELECT
    COALESCE(c.county, r.county) AS county,
    COALESCE(c.contact_count, 0) AS contact_count,
    COALESCE(r.report_count, 0) AS report_count,
    r.earliest_report,
    r.latest_report
FROM agg_contacts c
FULL OUTER JOIN agg_reports r ON c.county = r.county