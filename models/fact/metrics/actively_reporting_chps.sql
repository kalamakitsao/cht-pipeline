-- models/fact/chp_active_reporting_compliance.sql

{{ config(
    materialized = 'incremental', 
    unique_key = ['chp_area_id', 'period_id', 'metric_id'] 
) }}

-- Pre-filter and prepare visits from the source table
-- In a non-incremental model, all relevant historical data is processed.
WITH filtered_visits AS (
    SELECT
        v.reported_by_parent AS chp_area_id,
        v.household,
        v.reported,
        v.saved_timestamp
    FROM {{ ref('household_visit') }} v -- Assuming v1 is a source defined in your sources.yml
    JOIN {{ ref('dim_location') }} l ON l.location_id = v.reported_by_parent
    WHERE l.level = 'chp area' -- Filter for 'chp area' locations early for efficiency
),

-- Join each visit to all matching periods from the dim_period dimension table
-- and compute weekly/daily units based on period_type
period_chw_visits AS (
    SELECT
        p.period_id,
        p.label,
        p.period_type,
        p.start_date,
        p.end_date,
        fv.chp_area_id,
        CASE
            WHEN p.period_type = 'daily' THEN fv.reported::date
            ELSE date_trunc('week', fv.reported)::date
        END AS period_unit, -- The daily or weekly "unit" of reporting
        COUNT(DISTINCT fv.household) AS unique_households
    FROM {{ ref('dim_period') }} p
    JOIN filtered_visits fv
      ON fv.reported BETWEEN p.start_date AND p.end_date
    GROUP BY p.period_id, p.label, p.period_type, p.start_date, p.end_date, fv.chp_area_id, period_unit
),

-- Calculate compliance for each CHP area within each period
-- A "valid_unit" means the unique household visit threshold was met for that period_unit (day/week)
period_compliance AS (
    SELECT
        period_id,
        label,
        period_type,
        chp_area_id,
        COUNT(*) FILTER (
            WHERE
                (period_type = 'daily' AND unique_households >= 1) OR -- Daily: 1+ unique household visit
                (period_type <> 'daily' AND unique_households >= 5)   -- Weekly/Other: 5+ unique household visits
        ) AS valid_units,
        COUNT(*) AS total_units -- Total number of daily/weekly units within the period for a CHP
    FROM period_chw_visits
    GROUP BY period_id, label, period_type, chp_area_id
)

-- Final metric output: 'chp_actively_reporting' is 1 if all units were valid, else 0
SELECT
    chp_area_id,
    period_id,
    'chp_actively_reporting' AS metric_id,
    CASE
        WHEN valid_units = total_units AND total_units > 0 THEN 1 -- 1 if all units met compliance AND there was at least one unit to measure
        ELSE 0 -- 0 if not fully compliant or no units to measure (e.g., no visits in period)
    END AS value,
    CURRENT_TIMESTAMP AS last_updated -- This timestamp helps track when this compliance record was generated
FROM period_compliance