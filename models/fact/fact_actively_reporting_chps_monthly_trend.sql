-- Active CHPs
WITH thresholds AS (
  SELECT *
  FROM (VALUES
    -- min_ratio   min_abs_visits
    (0.165::float, 0)
  ) AS t(min_ratio, min_abs_visits)
),
scored AS (
  SELECT
    h.location_id,
    h.period_start,
    CASE
      WHEN COALESCE(v.value,0) >= th.min_abs_visits THEN 1
      WHEN COALESCE(h.value,0) > 0
           AND (COALESCE(v.value,0)::float / h.value) > th.min_ratio THEN 1
      ELSE 0
    END AS is_active
  FROM fact_households_registered_monthly_trend h
  CROSS JOIN thresholds th
  LEFT JOIN fact_households_visited_monthly_trend v
         ON h.location_id  = v.location_id
        AND h.period_start = v.period_start
  WHERE h.period_start >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
)

SELECT
  location_id,
  period_start AS period_id,
  'active_chps_new' AS metric_id,
  1 AS value,
  current_timestamp as last_updated
FROM scored
WHERE is_active = 1;
