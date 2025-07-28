{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    tags = ['kpi', 'sha'],
    on_schema_change = 'ignore'
) }}

-- Step 1: Filter relevant SHA records by period dates
WITH sha_filtered AS (
    SELECT
        sr.member_uuid,
        sr.reported_by_parent AS location_id,
        sr.has_sha_registration,
        sr.reported::date AS reported_date
    FROM {{ ref('sha_registration') }} sr
    JOIN {{ ref('dim_period_date_map') }} pd ON sr.reported::date = pd.date
),

-- Step 2: Join with patients to get household_id
patient_data AS (
    SELECT uuid, household_id
    FROM {{ ref('patient_f_client') }}
    WHERE uuid IS NOT NULL AND household_id IS NOT NULL
),

-- Step 3: For each household, keep latest SHA registration
latest_sha_registration AS (
    SELECT DISTINCT ON (p.household_id)
        p.household_id,
        s.location_id,
        s.reported_date,
        s.has_sha_registration
    FROM sha_filtered s
    INNER JOIN patient_data p ON s.member_uuid = p.uuid
    ORDER BY p.household_id, s.reported_date DESC
),

-- Step 4: Daily metrics
daily_metrics AS (
    SELECT
        location_id,
        reported_date AS date,
        COUNT(*) AS households_assessed_sha,
        COUNT(*) FILTER (WHERE has_sha_registration IS TRUE) AS households_with_sha
    FROM latest_sha_registration
    GROUP BY location_id, reported_date
),

-- Step 5: Map daily metrics to periods using period_date_map
mapped AS (
    SELECT
        d.location_id,
        pd.period_id,
        d.households_assessed_sha,
        d.households_with_sha
    FROM daily_metrics d
    JOIN {{ ref('dim_period_date_map') }} pd ON d.date = pd.date
),

-- Step 6: Aggregate & unpivot
aggregated AS (
    SELECT
        location_id,
        period_id,
        'households_assessed_sha' AS metric_id,
        SUM(households_assessed_sha) AS value
    FROM mapped
    GROUP BY location_id, period_id

    UNION ALL

    SELECT
        location_id,
        period_id,
        'households_with_sha' AS metric_id,
        SUM(households_with_sha) AS value
    FROM mapped
    GROUP BY location_id, period_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
