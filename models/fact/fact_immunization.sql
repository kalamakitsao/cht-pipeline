-- models/fact/fact_immunization.sql
{{ config(
  materialized = "table",
  indexes = [
    {'columns': ['location_id', 'period_id', 'metric_id'], 'unique': true},
    {'columns': ['period_id']},
    {'columns': ['metric_id']},
    {'columns': ['location_id']}
  ],
  tags = ["kpi","cadence_hourly"]
) }}

WITH children AS (
  SELECT
    c.chp_area_id AS location_id,
    c.period_id,
    c.patient_id,
    c.sex
  FROM {{ ref('children_turning_one') }} c
),

children_agg AS (
  SELECT
    c.location_id,
    c.period_id,
    COUNT(*) FILTER (WHERE c.sex = 'male')::bigint   AS male_turning_one,
    COUNT(*) FILTER (WHERE c.sex = 'female')::bigint AS female_turning_one,
    COUNT(*)::bigint                                 AS children_turning_one
  FROM children c
  GROUP BY 1, 2
),

latest_imm AS (
  SELECT
    lis.patient_id,
    lis.period_id,
    lis.sex,
    lis.has_measles_9
  FROM {{ ref('latest_immunization_status_enriched') }} lis
),

latest_imm_agg AS (
  SELECT
    c.location_id,
    c.period_id,
    COUNT(DISTINCT lis.patient_id)
      FILTER (WHERE lis.sex = 'male' AND lis.has_measles_9 = 'complete')::bigint
      AS fully_immunized_male,
    COUNT(DISTINCT lis.patient_id)
      FILTER (WHERE lis.sex = 'female' AND lis.has_measles_9 = 'complete')::bigint
      AS fully_immunized_female
  FROM children c
  JOIN latest_imm lis
    ON lis.patient_id = c.patient_id
   AND lis.period_id = c.period_id
  GROUP BY 1, 2
),

all_imm AS (
  SELECT
    air.patient_id,
    dp.period_id,
    air.sex,
    air.needs_immunization_referral,
    air.imm_schedule_upto_date,
    air.needs_growth_monitoring_referral,
    air.needs_deworming_follow_up
  FROM {{ ref('immunization_records_enriched') }} air
  JOIN {{ ref('dim_period') }} dp
    ON air.report_date BETWEEN dp.start_date AND dp.end_date
  WHERE dp.period_id_name <> 'today'
),

all_imm_agg AS (
  SELECT
    c.location_id,
    c.period_id,

    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE)::bigint
      AS referred_for_immunization,

    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE AND air.sex = 'male')::bigint
      AS referred_immunization_male,

    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE AND air.sex = 'female')::bigint
      AS referred_immunization_female,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_immunization_referral IS TRUE
          AND COALESCE(air.imm_schedule_upto_date, FALSE) IS NOT TRUE
          AND air.sex = 'male'
      )::bigint AS referred_missed_vaccine_male,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_immunization_referral IS TRUE
          AND COALESCE(air.imm_schedule_upto_date, FALSE) IS NOT TRUE
          AND air.sex = 'female'
      )::bigint AS referred_missed_vaccine_female,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_growth_monitoring_referral IS TRUE
          AND air.sex = 'male'
      )::bigint AS referred_growth_monitoring_male,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_growth_monitoring_referral IS TRUE
          AND air.sex = 'female'
      )::bigint AS referred_growth_monitoring_female,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_deworming_follow_up IS TRUE
          AND air.sex = 'male'
      )::bigint AS needs_deworming_follow_up_male,

    COUNT(DISTINCT air.patient_id)
      FILTER (
        WHERE air.needs_deworming_follow_up IS TRUE
          AND air.sex = 'female'
      )::bigint AS needs_deworming_follow_up_female

  FROM children c
  JOIN all_imm air
    ON air.patient_id = c.patient_id
   AND air.period_id = c.period_id
  GROUP BY 1, 2
),

all_keys AS (
  SELECT location_id, period_id FROM children_agg
  UNION
  SELECT location_id, period_id FROM latest_imm_agg
  UNION
  SELECT location_id, period_id FROM all_imm_agg
),

agg AS (
  SELECT
    k.location_id,
    k.period_id,

    COALESCE(ca.male_turning_one, 0)::bigint AS male_turning_one,
    COALESCE(ca.female_turning_one, 0)::bigint AS female_turning_one,
    COALESCE(ca.children_turning_one, 0)::bigint AS children_turning_one,

    COALESCE(li.fully_immunized_male, 0)::bigint AS fully_immunized_male,
    COALESCE(li.fully_immunized_female, 0)::bigint AS fully_immunized_female,

    COALESCE(ai.referred_for_immunization, 0)::bigint AS referred_for_immunization,
    COALESCE(ai.referred_immunization_male, 0)::bigint AS referred_immunization_male,
    COALESCE(ai.referred_immunization_female, 0)::bigint AS referred_immunization_female,

    COALESCE(ai.referred_missed_vaccine_male, 0)::bigint AS referred_missed_vaccine_male,
    COALESCE(ai.referred_missed_vaccine_female, 0)::bigint AS referred_missed_vaccine_female,

    COALESCE(ai.referred_growth_monitoring_male, 0)::bigint AS referred_growth_monitoring_male,
    COALESCE(ai.referred_growth_monitoring_female, 0)::bigint AS referred_growth_monitoring_female,

    COALESCE(ai.needs_deworming_follow_up_male, 0)::bigint AS needs_deworming_follow_up_male,
    COALESCE(ai.needs_deworming_follow_up_female, 0)::bigint AS needs_deworming_follow_up_female

  FROM all_keys k
  LEFT JOIN children_agg ca
    ON ca.location_id = k.location_id
   AND ca.period_id = k.period_id
  LEFT JOIN latest_imm_agg li
    ON li.location_id = k.location_id
   AND li.period_id = k.period_id
  LEFT JOIN all_imm_agg ai
    ON ai.location_id = k.location_id
   AND ai.period_id = k.period_id
),

metrics AS (
  SELECT
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  FROM agg a
  CROSS JOIN LATERAL (
    VALUES
      ('male_turning_one',                  a.male_turning_one),
      ('female_turning_one',                a.female_turning_one),
      ('children_turning_one',              a.children_turning_one),
      ('fully_immunized_male',              a.fully_immunized_male),
      ('fully_immunized_female',            a.fully_immunized_female),
      ('referred_for_immunization',         a.referred_for_immunization),
      ('referred_immunization_male',        a.referred_immunization_male),
      ('referred_immunization_female',      a.referred_immunization_female),
      ('referred_missed_vaccine_male',      a.referred_missed_vaccine_male),
      ('referred_missed_vaccine_female',    a.referred_missed_vaccine_female),
      ('referred_growth_monitoring_male',   a.referred_growth_monitoring_male),
      ('referred_growth_monitoring_female', a.referred_growth_monitoring_female),
      ('needs_deworming_follow_up_male',    a.needs_deworming_follow_up_male),
      ('needs_deworming_follow_up_female',  a.needs_deworming_follow_up_female)
  ) AS m(metric_id, value)
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0
