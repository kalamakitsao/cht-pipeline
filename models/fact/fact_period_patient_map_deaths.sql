{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

death_data AS (
    SELECT
        dr.reported_by_parent AS location_id,
        DATE(dr.reported) AS reported_date,
        dr.patient_id,
        dr.death_type,
        dr.patient_age_in_days
    FROM {{ ref('death_report') }} dr
),

mapped AS (
    SELECT
        p.period_id AS period_id,
        d.location_id,
        d.patient_id,
        unnest(
          ARRAY_REMOVE(ARRAY[
            CASE WHEN d.death_type = 'maternal death' THEN 'maternal_deaths' END,
            CASE WHEN d.patient_age_in_days < 29 THEN 'neonatal_deaths' END,
            CASE WHEN d.patient_age_in_days BETWEEN 29 AND 1827 THEN 'child_deaths' END,
            'total_deaths'
          ], NULL)
        ) AS metric_id
    FROM death_data d
    JOIN periods p ON d.reported_date BETWEEN p.start_date AND p.end_date
)

SELECT * FROM mapped
