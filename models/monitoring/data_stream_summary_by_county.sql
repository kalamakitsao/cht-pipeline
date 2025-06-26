{{ config(
    materialized = 'incremental',
    unique_key = ['county', 'snapshot_date'],
    on_schema_change = 'ignore',
    tags = ['daily_snapshot'],
    partition_by = {'field': 'snapshot_date', 'data_type': 'date'}
) }}

WITH snapshot_date AS (
    SELECT CURRENT_DATE AS snapshot_date
),

location_hierarchy AS (
    SELECT
        chp_area.location_id AS chp_area_id,
        county.name AS county
    FROM {{ ref('dim_location') }} chp_area
    LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
    LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
    LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
    WHERE chp_area.level = 'chp area'
      AND chp_area.name !~ '^[0-9]+$'
      AND county.name IS NOT NULL
),

contacts_by_county AS (
    SELECT
        lh.county,
        COUNT(*) AS contact_count
    FROM {{ ref('patient_f_client') }} c
    JOIN {{ ref('household') }} h ON c.household_id = h.uuid
    JOIN location_hierarchy lh ON lh.chp_area_id = h.chv_area_id
    GROUP BY lh.county
),

reports_by_county AS (
    SELECT
        lh.county,
        COUNT(*) AS report_count,
        MIN(d.reported) AS earliest_report,
        MAX(d.reported) AS latest_report
    FROM {{ ref('data_record') }} d
    JOIN location_hierarchy lh ON lh.chp_area_id = d.parent_uuid
    GROUP BY lh.county
),

aggregated AS (
    SELECT
        COALESCE(c.county, r.county) AS county,
        COALESCE(c.contact_count, 0) AS contact_count,
        COALESCE(r.report_count, 0) AS report_count,
        r.earliest_report,
        r.latest_report
    FROM contacts_by_county c
    FULL OUTER JOIN reports_by_county r ON c.county = r.county
),

filtered AS (
    SELECT *
    FROM aggregated
    WHERE county IS NOT NULL
      AND county !~* 'janet.*'  -- exclude misclassified areas
)

SELECT
    county,
    contact_count,
    report_count,
    earliest_report,
    latest_report,
    sd.snapshot_date,
    CURRENT_TIMESTAMP AS last_updated
FROM filtered
CROSS JOIN snapshot_date sd
