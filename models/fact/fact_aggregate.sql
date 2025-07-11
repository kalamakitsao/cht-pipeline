-- models/fact/fact_aggregate.sql
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_metric_id ON {{ this }} (metric_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_period_id ON {{ this }} (period_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_location_id ON {{ this }} (location_id)"
    ]
) }}

{% set metric_models = [
    'population',
    'u5_conditions',
    'sha_metrics',
    'chps_enrolled',
    'households_registered',
    'under_1_immunised',
    'people_served',
    'referrals',
    'deaths',
    'pregnancy_metrics',
    'households_visited',
    'ncd_metrics',
    'chps_with_households',
    'community_events_participation',
    'revised_active_chps'
] %}

WITH
{% for model in metric_models %}
  {% if not loop.first %},{% endif %}
  {{ model }}_ranked AS (
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY location_id, period_id, metric_id
                 ORDER BY snapshot_date DESC NULLS LAST, last_updated DESC
               ) AS rn
        FROM {{ ref(model) }}
      ) sub
      WHERE rn = 1
  )
{% endfor %}

SELECT * FROM population_ranked
UNION ALL
SELECT * FROM u5_conditions_ranked
UNION ALL
SELECT * FROM sha_metrics_ranked
UNION ALL
SELECT * FROM chps_enrolled_ranked
UNION ALL
SELECT * FROM households_registered_ranked
UNION ALL
SELECT * FROM under_1_immunised_ranked
UNION ALL
SELECT * FROM people_served_ranked
UNION ALL
SELECT * FROM referrals_ranked
UNION ALL
SELECT * FROM deaths_ranked
UNION ALL
SELECT * FROM pregnancy_metrics_ranked
UNION ALL
SELECT * FROM households_visited_ranked
UNION ALL
SELECT * FROM ncd_metrics_ranked
UNION ALL
SELECT * FROM chps_with_households_ranked
UNION ALL
SELECT * FROM community_events_participation_ranked
UNION ALL
SELECT * FROM revised_active_chps_ranked
