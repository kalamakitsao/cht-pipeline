{{ config(
  materialized = 'incremental',
  unique_key = ['county', 'period_id'],
  on_schema_change = 'append_new_columns',
  tags = ['snapshot', 'daily_summary']
) }}

WITH chp_areas AS (
  SELECT
    chp.location_id AS chp_area_id,
    county.name AS county
  FROM {{ ref('dim_location') }} chp
  LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp.parent_id
  LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
  LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
  WHERE chp.level = 'chp area'
    AND chp.name !~ '^[0-9]+$'
),

periods AS (
  SELECT * FROM {{ ref('dim_period') }}
),

-- Contact counts from registered clients
client_contacts AS (
  SELECT
    hh.chv_area_id AS chp_area_id,
    p.period_id,
    COUNT(clients.uuid) AS contact_count
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.reported <= p.end_date
  WHERE hh.chv_area_id IS NOT NULL
  GROUP BY hh.chv_area_id, p.period_id
),

-- Reports within the last 3 days
recent_reports AS (
  SELECT
    d.parent_uuid AS chp_area_id,
    p.period_id,
    COUNT(*) AS report_count,
    MIN(d.reported) AS earliest_report,
    MAX(d.reported) AS latest_report
  FROM {{ ref('data_record') }} d
  JOIN periods p ON d.reported BETWEEN p.start_date AND p.end_date
  WHERE d.reported >= CURRENT_DATE - INTERVAL '3 days'
    AND d.parent_uuid IS NOT NULL
  GROUP BY d.parent_uuid, p.period_id
),

-- Combine metrics by chp_area_id
aggregated AS (
  SELECT
    COALESCE(cc.period_id, rr.period_id) AS period_id,
    ca.county,
    COALESCE(cc.contact_count, 0) AS contact_count,
    COALESCE(rr.report_count, 0) AS report_count,
    rr.earliest_report,
    rr.latest_report
  FROM chp_areas ca
  LEFT JOIN client_contacts cc ON cc.chp_area_id = ca.chp_area_id
  LEFT JOIN recent_reports rr ON rr.chp_area_id = ca.chp_area_id AND rr.period_id = cc.period_id
)

SELECT
  county,
  period_id,
  contact_count,
  report_count,
  earliest_report,
  latest_report,
  CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE county IS NOT NULL
