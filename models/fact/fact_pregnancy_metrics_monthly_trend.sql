{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','pregnancy','pnc','cadence_daily'],
  on_schema_change = 'ignore',
  indexes = [
    {"columns": ["period_start","location_id","metric_id"]},
    {"columns": ["metric_id"]},
    {"columns": ["location_id","period_start"]}
  ]
) }}

WITH phv_src AS (
  SELECT
    phv.reported_by_parent                 AS location_id,
    phv.reported::date                     AS report_date,
    date_trunc('month', reported) AS period_start,
    phv.patient_id,
    phv.patient_age_in_years,
    phv.is_currently_pregnant,
    phv.is_new_pregnancy,
    phv.has_been_referred,
    phv.has_started_anc,
    phv.is_anc_upto_date,
    phv.current_edd
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }} phv
  {% if is_incremental() %}
    -- Narrow scan on incremental runs; widen if you expect late arrivals
    WHERE phv.reported >= date_trunc('month', current_date) - interval '12 months'
  {% endif %}
),

pnc_src AS (
  SELECT
    pnc.reported_by_parent                 AS location_id,
    pnc.reported::date                     AS report_date,
    date_trunc('month', reported) AS period_start,
    pnc.patient_id,
    pnc.place_of_delivery,
    pnc.pnc_service_count,
    pnc.date_of_delivery,
    pnc.is_referred_for_pnc_services
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'postnatal_care_service') }} pnc
  {% if is_incremental() %}
    WHERE pnc.reported >= date_trunc('month', current_date) - interval '12 months'
  {% endif %}
),

phv_exploded AS (
  SELECT
    p.location_id,
    p.period_start,
    p.patient_id,
    m.metric_id
  FROM phv_src p
  CROSS JOIN LATERAL (
    VALUES
      ('currently_pregnant',                 (COALESCE(p.is_currently_pregnant,false) OR COALESCE(p.is_new_pregnancy,false))),
      ('new_pregnancies',                    (COALESCE(p.is_new_pregnancy,false))),
      ('new_teen_pregnancies',               (COALESCE(p.is_new_pregnancy,false) AND p.patient_age_in_years BETWEEN 10 AND 19)),
      ('teen_pregnancies',                   ((COALESCE(p.is_currently_pregnant,false) OR COALESCE(p.is_new_pregnancy,false)) AND p.patient_age_in_years BETWEEN 10 AND 19)),
      ('pregnant_women_visited',             TRUE),
      ('pregnant_women_referred',            (COALESCE(p.has_been_referred,false))),
      ('pregnant_women_referred_anc',        (COALESCE(p.has_been_referred,false) AND COALESCE(p.is_anc_upto_date,false) = FALSE)),
      ('new_pregnant_women_referred_anc',    (COALESCE(p.is_new_pregnancy,false) AND COALESCE(p.has_been_referred,false) AND COALESCE(p.has_started_anc,false) = FALSE)),
      ('new_teen_pregnant_women_referred_anc',(COALESCE(p.is_new_pregnancy,false) AND COALESCE(p.has_been_referred,false) AND COALESCE(p.has_started_anc,false) = FALSE AND p.patient_age_in_years BETWEEN 10 AND 19)),
      -- first trimester computed relative to the report date, not CURRENT_DATE
      ('first_trimester_pregnancies',
         (COALESCE(p.is_new_pregnancy,false)
          AND p.current_edd IS NOT NULL
          AND ( 40 - ((p.current_edd::date - p.report_date)::numeric / 7.0) ) BETWEEN 0 AND 12))
  ) AS m(metric_id, include_flag)
  WHERE m.include_flag
),

phv_dedup AS (
  SELECT DISTINCT location_id, period_start, metric_id, patient_id
  FROM phv_exploded
),

phv_monthly AS (
  SELECT location_id, period_start, metric_id, COUNT(*)::bigint AS value
  FROM phv_dedup
  GROUP BY 1,2,3
),

pnc_exploded AS (
  SELECT
    p.location_id,
    p.period_start,
    p.patient_id,
    m.metric_id
  FROM pnc_src p
  CROSS JOIN LATERAL (
    VALUES
      ('deliveries',               (p.pnc_service_count = 1)),
      ('skilled_birth_attendance', (p.place_of_delivery = 'health_facility')),
      ('referred_pnc',             (COALESCE(p.is_referred_for_pnc_services,false)))
  ) AS m(metric_id, include_flag)
  WHERE m.include_flag
),

pnc_dedup AS (
  SELECT DISTINCT location_id, period_start, metric_id, patient_id
  FROM pnc_exploded
),

pnc_monthly AS (
  SELECT location_id, period_start, metric_id, COUNT(*)::bigint AS value
  FROM pnc_dedup
  GROUP BY 1,2,3
),

latest_phv_with_edd AS (
  SELECT DISTINCT ON (phv.patient_id)
         phv.patient_id,
         phv.reported_by_parent      AS location_id,
         phv.current_edd,
         phv.reported::date          AS reported_date,
         date_trunc('month', phv.reported) AS period_start
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }} phv
  WHERE phv.is_currently_pregnant = TRUE
    AND phv.current_edd IS NOT NULL
  ORDER BY phv.patient_id, phv.reported DESC
),

pregnancy_window AS (
  SELECT
    patient_id,
    location_id,
    current_edd,
    (current_edd - INTERVAL '9 months')::date AS pregnancy_start,
    period_start
  FROM latest_phv_with_edd
),

actively_pregnant AS (
  SELECT
    pw.location_id,
    pw.period_start,
    'actively_pregnant_women'::text AS metric_id,
    COUNT(DISTINCT pw.patient_id)::bigint AS value
  FROM pregnancy_window pw WHERE pw.pregnancy_start < pw.period_start
   AND pw.current_edd     >= pw.period_start
  GROUP BY 1,2,3
),

latest_delivery AS (
  SELECT
    patient_id,
    MAX(date_of_delivery) AS last_delivery_date
  FROM pnc_src
  WHERE date_of_delivery IS NOT NULL
  GROUP BY 1
),

repeat_pregnancies AS (
  SELECT
    p.location_id,
    p.period_start,
    'repeat_pregnancies'::text AS metric_id,
    COUNT(DISTINCT p.patient_id)::bigint AS value
  FROM phv_src p
  JOIN latest_delivery d
    ON d.patient_id = p.patient_id
  WHERE COALESCE(p.is_new_pregnancy,false) = TRUE
    AND p.report_date BETWEEN d.last_delivery_date + INTERVAL '1 day'
                          AND d.last_delivery_date + INTERVAL '12 months'
  GROUP BY 1,2,3
),

all_monthly AS (
  SELECT * FROM phv_monthly
  UNION ALL
  SELECT * FROM pnc_monthly
  UNION ALL
  SELECT * FROM actively_pregnant
  UNION ALL
  SELECT * FROM repeat_pregnancies
)

SELECT
  location_id,
  period_start,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM all_monthly
WHERE value > 0
