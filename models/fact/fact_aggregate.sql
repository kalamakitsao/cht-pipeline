-- models/fact/fact_aggregate.sql
{{ config(
    materialized='table',
    on_schema_change='append_new_columns',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_metric_id ON {{ this }} (metric_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_period_id ON {{ this }} (period_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_location_id ON {{ this }} (location_id)"
    ]
) }}

{% set metric_models = [
    'agg_population_metrics_rolling',
    'agg_u5_metrics_rolling',
    'agg_sha_metrics_rolling',
    'agg_chps_enrolled_metrics_rolling',
    'agg_households_registered_and_chp_counts_rolling',
    'agg_under_1_immunised_metrics_rolling',
    'agg_people_served_metrics_rolling',
    'agg_referral_metrics_rolling',
    'agg_death_metrics_rolling',
    'agg_maternal_metrics_rolling',
    'agg_households_visited_metrics_rolling',
    'agg_over_five_metrics_rolling',
    'agg_community_events_metrics_rolling',
    'agg_revised_active_chps_metrics_rolling'
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
                ORDER BY snapshot_date DESC NULLS LAST, last_updated DESC NULLS LAST
              )
              AS rn
        FROM {{ ref(model) }}
      ) sub
      WHERE rn = 1
  )
{% endfor %}

SELECT * FROM agg_population_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_u5_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_sha_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_chps_enrolled_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_households_registered_and_chp_counts_rolling_ranked
UNION ALL
SELECT * FROM agg_under_1_immunised_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_people_served_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_referral_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_death_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_maternal_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_households_visited_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_over_five_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_community_events_metrics_rolling_ranked
UNION ALL
SELECT * FROM agg_revised_active_chps_metrics_rolling_ranked
