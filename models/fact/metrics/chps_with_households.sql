-- models/fact/metrics/chps_with_households.sql
-- Add logic to count CHPs who registered a household
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH chp_households AS (
    SELECT DISTINCT
        chv_area_id AS location_id,
        DATE(reported) AS reported_date
    FROM {{ ref('household') }}
    WHERE chv_area_id IN (SELECT location_id FROM {{ ref('dim_location') }})
),

matched_periods AS (
    SELECT
        ch.location_id,
        p.period_id
    FROM chp_households ch
    JOIN {{ ref('dim_period') }} p
      ON ch.reported_date <= p.end_date
),

aggregated AS (
    SELECT
        location_id,
        period_id,
        COUNT(DISTINCT location_id) AS value
    FROM matched_periods
    GROUP BY location_id, period_id
)

SELECT
    location_id,
    period_id,
    'chps_with_hholds' AS metric_id,
    1 AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
