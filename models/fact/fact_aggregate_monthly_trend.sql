-- Active CHPs
WITH months thresholds AS (
  SELECT *
  FROM (VALUES
    -- period_id_name          min_ratio   min_abs_visits
    (0.165::float, 0),
  ) AS t(min_ratio, min_abs_visits)
),

SELECT
    location_id,
    period_start,
    metric_id,
    value
FROM cumulative
ORDER BY location_id, period_start;

scored AS (
  SELECT
    h.location_id,
    m.period_start,
    CASE
      WHEN COALESCE(v.value,0) >= th.min_abs_visits THEN 1
      WHEN COALESCE(h.value,0) > 0
           AND (COALESCE(v.value,0)::float / h.value) > th.min_ratio THEN 1
      ELSE 0
    END AS is_active
  FROM fact_households_registered_monthly_trend h, thresholds th
  -- visits drive activity; if there are no visits, only the “abs>0” rule would pass anyway
  LEFT JOIN fact_households_visited_monthly_trend v
         ON h.location_id = v.location_id
        AND h.period_start   = m.period_start
  WHERE m.period_start >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
)

SELECT
  location_id,
  period_id,
  'active_chps_new' AS metric_id,
  1                 AS value
FROM scored
WHERE is_active = 1
-- Households visited
UNION ALL
select * from fact_households_visited_monthly_trend
-- Population served
UNION ALL
select * from fact_people_served_monthly_trend
-- Total Referrals
UNION ALL
select * from fact_people_served_monthly_trend
-- NCDs screening
UNION ALL
select * from fact_referrals_monthly_trend
-- Child health illnesses
UNION ALL
select * from fact_referrals_monthly_trend
