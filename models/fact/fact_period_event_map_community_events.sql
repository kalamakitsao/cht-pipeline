{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'event_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_events AS (
  SELECT
    ce.uuid AS event_id,
    ce.reported_by_parent AS location_id,
    DATE(ce.event_date) AS event_date,
    ce.event_types
  FROM {{ ref('community_event') }} ce
),

mapped AS (
  SELECT
    p.period_id AS period_id,
    e.location_id,
    e.event_id,
    unnest(ARRAY_REMOVE(ARRAY[
      CASE WHEN e.event_types ILIKE '%monthly_cu_meetings%' THEN 'monthly_cu_meetings' END,
      CASE WHEN e.event_types IS NOT NULL AND (
        (e.event_types ILIKE '%monthly_cu_meetings%' AND e.event_types <> 'monthly_cu_meetings')
        OR e.event_types NOT ILIKE '%monthly_cu_meetings%'
      ) THEN 'other_community_events' END
    ], NULL)) AS metric_id
  FROM base_events e
  JOIN periods p ON e.event_date BETWEEN p.start_date AND p.end_date
)

SELECT * FROM mapped
