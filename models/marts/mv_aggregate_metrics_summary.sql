-- models/metrics/mv_aggregate_metrics_summary.sql

{{ config(
    materialized = 'table',
    indexes = [
      -- Unique constraint: one aggregated row per location x period x metric
      {"columns": ["location_id", "period_id", "metric_id"], "unique": true},
      -- PRIMARY FIX: full hierarchy text-name composite index.
      -- The API filters on text names (not IDs) across all four hierarchy levels
      -- plus period_label. Without this, Postgres bitmap-scans ~5 million rows on
      -- every "All Time" query, then filters out 624k rows per worker (37s query).
      -- With this index, the planner does a direct Index Scan returning ~72 rows.
      {"columns": ["county", "sub_county", "community_unit", "chp_area", "period_label"]},
      -- Narrower variants for panels that filter at a higher hierarchy level
      {"columns": ["county", "period_label"]},
      {"columns": ["sub_county", "period_label"]},
      {"columns": ["community_unit", "period_label"]},
      -- ID-based path used by mart_trends when joining this table internally
      {"columns": ["chp_area_id", "period_label"]},
      -- Retained existing supporting indexes
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ],
    on_schema_change = 'append_new_columns',
    tags = ['cadence_hourly']
) }}

WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        chp_area,
        community_unit_id,
        community_unit,
        sub_county_id,
        sub_county,
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
),
aggregates AS (
    SELECT
        fa.location_id,
        fa.period_id,
        fa.metric_id,
        fa.value,
        fa.last_updated
    FROM {{ ref('fact_aggregate') }} fa
),
joined AS (
    SELECT
        lh.chp_area_id,
        lh.chp_area,
        lh.community_unit_id,
        lh.community_unit,
        lh.sub_county_id,
        lh.sub_county,
        lh.county_id,
        lh.county,
        dp.start_date AS period_start,
        dp.end_date AS period_end,
        dp.label AS period_label,
        dm.group_name AS metric_group,
        dm.metric_group_id AS metric_group_id,
        dm.name AS metric,
        a.value,
        'chp area' AS level,
        a.location_id,
        a.period_id,
        a.metric_id,
        a.last_updated
    FROM aggregates a
    JOIN location_hierarchy lh ON lh.chp_area_id = a.location_id
    JOIN {{ ref('dim_period') }} dp ON dp.period_id = a.period_id
    JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = a.metric_id
)
SELECT
    level,
    chp_area_id,
    chp_area,
    community_unit_id,
    community_unit,
    sub_county_id,
    sub_county,
    county_id,
    county,
    period_start,
    period_end,
    period_label,
    metric_group,
    metric_group_id,
    metric,
    value,
    location_id,
    period_id,
    metric_id,
    last_updated
FROM joined
