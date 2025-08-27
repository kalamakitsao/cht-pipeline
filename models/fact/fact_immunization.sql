-- models/fact/fact_immunization.sql
{{ config(
  materialized = "table",
  tags = ["kpi","cadence_hourly"]
) }}

WITH children AS (
  SELECT
    c.chp_area_id  AS location_id,
    c.period_id,
    c.patient_id,
    c.sex
  FROM {{ ref('children_turning_one') }} c
),

latest_imm AS (
  SELECT
    lis.patient_id,
    lis.period_id,
    lis.sex,
    lis.has_measles_9
  FROM {{ ref('latest_immunization_status_enriched') }} lis
),

all_imm AS (
  SELECT
    air.patient_id,
    air.period_id,
    air.sex,
    air.needs_immunization_referral,      -- ideally boolean in *_enriched
    air.imm_schedule_upto_date,            -- 'yes' / 'no' / NULL
    air.needs_growth_monitoring_referral,  -- boolean preferred
    air.needs_deworming_follow_up          -- 'yes' / 'no'
  FROM {{ ref('immunization_records_enriched') }} air
),

-- Aggregate once per (location_id, period_id)
agg AS (
  SELECT
    c.location_id,
    c.period_id,

    -- Children turning 1
    COUNT(*) FILTER (WHERE c.sex = 'male')   ::bigint AS male_turning_one,
    COUNT(*) FILTER (WHERE c.sex = 'female') ::bigint AS female_turning_one,
    COUNT(*)                                 ::bigint AS children_turning_one,

    -- Fully immunized (distinct patients from latest status)
    COUNT(DISTINCT lis.patient_id)
      FILTER (WHERE lis.sex = 'male'   AND lis.has_measles_9 = 'complete')  ::bigint AS fully_immunized_male,
    COUNT(DISTINCT lis.patient_id)
      FILTER (WHERE lis.sex = 'female' AND lis.has_measles_9 = 'complete')  ::bigint AS fully_immunized_female,

    -- Referred for immunization (distinct patients across all_imm)
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE)                ::bigint AS referred_for_immunization,
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE AND air.sex = 'male')
                                                                           ::bigint AS referred_immunization_male,
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE AND air.sex = 'female')
                                                                           ::bigint AS referred_immunization_female,

    -- Missed vaccines among those referred
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE
              AND COALESCE(air.imm_schedule_upto_date, 'no') <> 'yes'
              AND air.sex = 'male')                                         ::bigint AS referred_missed_vaccine_male,
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_immunization_referral IS TRUE
              AND COALESCE(air.imm_schedule_upto_date, 'no') <> 'yes'
              AND air.sex = 'female')                                       ::bigint AS referred_missed_vaccine_female,

    -- Growth monitoring referrals
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_growth_monitoring_referral IS TRUE
              AND air.sex = 'male')                                         ::bigint AS referred_growth_monitoring_male,
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_growth_monitoring_referral IS TRUE
              AND air.sex = 'female')                                       ::bigint AS referred_growth_monitoring_female,

    -- Deworming follow-up
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_deworming_follow_up = 'yes' AND air.sex = 'male')
                                                                           ::bigint AS needs_deworming_follow_up_male,
    COUNT(DISTINCT air.patient_id)
      FILTER (WHERE air.needs_deworming_follow_up = 'yes' AND air.sex = 'female')
                                                                           ::bigint AS needs_deworming_follow_up_female
  FROM children c
  LEFT JOIN latest_imm lis
    ON lis.patient_id = c.patient_id AND lis.period_id = c.period_id
  LEFT JOIN all_imm air
    ON air.patient_id = c.patient_id AND air.period_id = c.period_id
  GROUP BY c.location_id, c.period_id
),

-- Pivot columns → metric rows without arrays/records
metrics AS (
  SELECT
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  FROM agg a
  CROSS JOIN LATERAL (
    VALUES
      ('male_turning_one',                 a.male_turning_one),
      ('female_turning_one',               a.female_turning_one),
      ('children_turning_one',             a.children_turning_one),
      ('fully_immunized_male',             a.fully_immunized_male),
      ('fully_immunized_female',           a.fully_immunized_female),
      ('referred_for_immunization',        a.referred_for_immunization),
      ('referred_immunization_male',       a.referred_immunization_male),
      ('referred_immunization_female',     a.referred_immunization_female),
      ('referred_missed_vaccine_male',     a.referred_missed_vaccine_male),
      ('referred_missed_vaccine_female',   a.referred_missed_vaccine_female),
      ('referred_growth_monitoring_male',  a.referred_growth_monitoring_male),
      ('referred_growth_monitoring_female',a.referred_growth_monitoring_female),
      ('needs_deworming_follow_up_male',   a.needs_deworming_follow_up_male),
      ('needs_deworming_follow_up_female', a.needs_deworming_follow_up_female)
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
