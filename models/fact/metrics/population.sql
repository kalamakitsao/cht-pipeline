{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH periods AS (
    -- Select all columns from the dim_period reference model
    SELECT
        period_id,
        start_date,
        end_date
    FROM {{ ref('dim_period') }}
),

valid_locations AS (
    -- Select valid location IDs from the dim_location reference model
    SELECT location_id
    FROM {{ ref('dim_location') }}
),

-- CTE to get comprehensive client data, joined with household, periods, and valid locations.
-- This avoids redundant joins later by pre-calculating relevant flags.
client_data AS (
    SELECT
        hh.chv_area_id AS location_id,
        p.period_id,
        clients.uuid AS client_uuid,
        clients.sex,
        clients.reported,
        clients.date_of_birth,
        clients.muted,
        -- Determine if the client is under 5 years old for the given period's end date
        CASE
            WHEN clients.date_of_birth IS NOT NULL
                AND AGE(p.end_date, clients.date_of_birth) < INTERVAL '5 years'
                THEN TRUE
            ELSE FALSE
        END AS is_under_5
    FROM {{ ref('patient_f_client') }} clients
    JOIN {{ ref('household') }} hh
        ON clients.household_id = hh.uuid
    JOIN periods p
        ON clients.reported <= p.end_date -- Client reported by or before the period end date
    JOIN valid_locations l
        ON hh.chv_area_id = l.location_id
),

-- CTE to get death data, joined with periods and valid locations.
-- Includes a flag for under 5 deaths.
death_data AS (
    SELECT
        d.reported_by_parent AS location_id,
        p.period_id,
        d.uuid AS death_uuid,
        pax.sex, -- Get sex of the deceased from patient_f_client
        d.patient_age_in_days,
        -- Determine if the deceased client was under 5 years old (less than 1827 days)
        CASE
            WHEN d.patient_age_in_days < 1827 THEN TRUE
            ELSE FALSE
        END AS is_under_5
    FROM {{ ref('death_report') }} d
    JOIN periods p
        ON d.date_of_death BETWEEN p.start_date AND p.end_date
    JOIN valid_locations l
        ON d.reported_by_parent = l.location_id
    JOIN {{ ref('patient_f_client') }} pax
        ON d.patient_id = pax.uuid -- Join to get patient details like sex
),

-- Aggregate all necessary counts (registered, deaths, muted) using conditional aggregation.
-- This performs all counts in a single pass grouped by location and period.
aggregated_data AS (
    SELECT
        cd.location_id,
        cd.period_id,
        -- Total registered clients
        COUNT(DISTINCT cd.client_uuid) AS pax_registered,
        -- Registered male clients
        COUNT(DISTINCT CASE WHEN cd.sex = 'male' THEN cd.client_uuid END) AS pax_registered_male,
        -- Registered female clients
        COUNT(DISTINCT CASE WHEN cd.sex = 'female' THEN cd.client_uuid END) AS pax_registered_female,
        -- Registered clients under 5
        COUNT(DISTINCT CASE WHEN cd.is_under_5 THEN cd.client_uuid END) AS pax_registered_u5,
        -- Registered male clients under 5
        COUNT(DISTINCT CASE WHEN cd.sex = 'male' AND cd.is_under_5 THEN cd.client_uuid END) AS pax_registered_u5_male,
        -- Registered female clients under 5
        COUNT(DISTINCT CASE WHEN cd.sex = 'female' AND cd.is_under_5 THEN cd.client_uuid END) AS pax_registered_u5_female,

        -- Total muted clients within the period
        COUNT(DISTINCT CASE
            WHEN cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' -- Regex to ensure date format
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted,
        -- Muted male clients within the period
        COUNT(DISTINCT CASE
            WHEN cd.sex = 'male'
            AND cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted_male,
        -- Muted female clients within the period
        COUNT(DISTINCT CASE
            WHEN cd.sex = 'female'
            AND cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted_female,
        -- Muted clients under 5 within the period
        COUNT(DISTINCT CASE
            WHEN cd.is_under_5
            AND cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted_u5,
        -- Muted male clients under 5 within the period
        COUNT(DISTINCT CASE
            WHEN cd.sex = 'male' AND cd.is_under_5
            AND cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted_u5_male,
        -- Muted female clients under 5 within the period
        COUNT(DISTINCT CASE
            WHEN cd.sex = 'female' AND cd.is_under_5
            AND cd.muted IS NOT NULL
            AND cd.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND cd.muted::DATE BETWEEN p.start_date AND p.end_date
            THEN cd.client_uuid
        END) AS pax_muted_u5_female,

        -- Total deaths within the period
        COUNT(DISTINCT dd.death_uuid) AS pax_deaths,
        -- Male deaths within the period
        COUNT(DISTINCT CASE WHEN dd.sex = 'male' THEN dd.death_uuid END) AS pax_deaths_male,
        -- Female deaths within the period
        COUNT(DISTINCT CASE WHEN dd.sex = 'female' THEN dd.death_uuid END) AS pax_deaths_female,
        -- Deaths under 5 within the period
        COUNT(DISTINCT CASE WHEN dd.is_under_5 THEN dd.death_uuid END) AS pax_deaths_u5,
        -- Male deaths under 5 within the period
        COUNT(DISTINCT CASE WHEN dd.sex = 'male' AND dd.is_under_5 THEN dd.death_uuid END) AS pax_deaths_u5_male,
        -- Female deaths under 5 within the period
        COUNT(DISTINCT CASE WHEN dd.sex = 'female' AND dd.is_under_5 THEN dd.death_uuid END) AS pax_deaths_u5_female
    FROM client_data cd
    LEFT JOIN death_data dd
        ON cd.location_id = dd.location_id
        AND cd.period_id = dd.period_id
        AND cd.client_uuid = dd.death_uuid -- Assuming client_uuid and death_uuid (patient_id) are the same UUIDs
    JOIN periods p ON cd.period_id = p.period_id -- Join back to periods to use start_date/end_date for muted filter
    GROUP BY
        cd.location_id,
        cd.period_id
)

-- Final SELECT statements using UNION ALL to create a single fact table

-- Total Population
SELECT
    location_id,
    period_id,
    'population' AS metric_id,
    pax_registered - COALESCE(pax_deaths, 0) - COALESCE(pax_muted, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered - COALESCE(pax_deaths, 0) - COALESCE(pax_muted, 0) > 0

UNION ALL

-- Male Population
SELECT
    location_id,
    period_id,
    'population_male' AS metric_id,
    pax_registered_male - COALESCE(pax_deaths_male, 0) - COALESCE(pax_muted_male, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered_male - COALESCE(pax_deaths_male, 0) - COALESCE(pax_muted_male, 0) > 0

UNION ALL

-- Female Population
SELECT
    location_id,
    period_id,
    'population_female' AS metric_id,
    pax_registered_female - COALESCE(pax_deaths_female, 0) - COALESCE(pax_muted_female, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered_female - COALESCE(pax_deaths_female, 0) - COALESCE(pax_muted_female, 0) > 0

UNION ALL

-- Population Under 5
SELECT
    location_id,
    period_id,
    'population_under_5' AS metric_id,
    pax_registered_u5 - COALESCE(pax_deaths_u5, 0) - COALESCE(pax_muted_u5, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered_u5 - COALESCE(pax_deaths_u5, 0) - COALESCE(pax_muted_u5, 0) > 0

UNION ALL

-- Male Population Under 5
SELECT
    location_id,
    period_id,
    'population_under_5_male' AS metric_id,
    pax_registered_u5_male - COALESCE(pax_deaths_u5_male, 0) - COALESCE(pax_muted_u5_male, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered_u5_male - COALESCE(pax_deaths_u5_male, 0) - COALESCE(pax_muted_u5_male, 0) > 0

UNION ALL

-- Female Population Under 5
SELECT
    location_id,
    period_id,
    'population_under_5_female' AS metric_id,
    pax_registered_u5_female - COALESCE(pax_deaths_u5_female, 0) - COALESCE(pax_muted_u5_female, 0) AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated_data
WHERE pax_registered_u5_female - COALESCE(pax_deaths_u5_female, 0) - COALESCE(pax_muted_u5_female, 0) > 0
