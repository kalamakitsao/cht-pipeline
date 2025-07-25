-- models/ntd_campaign_targets.sql
{{
  config(
    materialized = 'table',
    unique_key = ['chu_code', 'cycle_name'],
    tags = ['ntd', 'targets']
  )
}}

WITH campaign_cycles AS (
  SELECT 
    cycle_name,
    start_date,
    UNNEST(target_counties) AS target_county
  FROM {{ ref('ntd_campaign_dates') }}
),

-- Get the complete CHP hierarchy for target counties
chp_hierarchy AS (
  SELECT
    ch.county AS county_name,
    ch.sub_county AS sub_county_name,
    ch.community_unit AS chu_name,
    ch.chp_area_uuid AS chp_area_uuid,
    ch.chp_area_name AS chp_area_name
  FROM
    {{ ref('mv_location_hierarchy') }} AS ch
    JOIN campaign_cycles cc ON ch.county_name = cc.target_county
),

-- Calculate age in months at campaign start for each person
population_with_age_at_campaign AS (
  SELECT
    p.uuid,
    p.household_id,
    p.sex,
    cc.cycle_name,
    cc.start_date,
    -- Calculate age in months at campaign start
    (DATE_PART('year', cc.start_date) - DATE_PART('year', p.date_of_birth)) * 12 +
    (DATE_PART('month', cc.start_date) - DATE_PART('month', p.date_of_birth)) AS age_in_months_at_campaign,
    ch.county_name,
    ch.sub_county_name,
    ch.chu_name,
    ch.chp_area_uuid,
    ch.chp_area_name
  FROM
    v1.patient_f_client p
    JOIN {{ ref('household') }} hh on p.household_id = hh.uuid
    JOIN {{ ref('chp_hierarchy') }} ch ON hh.chv_area_id = ch.chp_area_uuid
    JOIN campaign_cycles cc ON ch.county_name = cc.target_county

  WHERE
    p.date_of_birth IS NOT NULL
),

-- Aggregate target populations by CHP and campaign cycle
target_population_aggregated AS (
  SELECT
    cycle_name,
    start_date,
    county_name,
    sub_county_name,
    chu_name,
    chp_area_uuid,
    chp_area_name,
    -- Total population counts
    COUNT(DISTINCT uuid) AS count_total_population,
    -- Age group breakdowns
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 60) AS count_total_above_5_years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 12) AS count_total_above_1_years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign < 6) AS count_total_below_6months,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign < 6 AND sex = 'male') AS count_total_below_6months_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign < 6 AND sex = 'female') AS count_total_below_6months_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 6 AND 84) AS count_total_btn_6months_and_7years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 6 AND 84 AND sex = 'male') AS count_total_btn_6months_and_7years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 6 AND 84 AND sex = 'female') AS count_total_btn_6months_and_7years_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 84) AS count_total_above_7years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 84 AND sex = 'male') AS count_total_above_7years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 84 AND sex = 'female') AS count_total_above_7years_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 12 AND 59) AS count_total_1years_to_4years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 12 AND 59 AND sex = 'male') AS count_total_1years_to_4years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 12 AND 59 AND sex = 'female') AS count_total_1years_to_4years_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 24 AND 59) AS count_total_2years_to_5years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 24 AND 59 AND sex = 'male') AS count_total_2years_to_5years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 24 AND 59 AND sex = 'female') AS count_total_2years_to_5years_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 60 AND 168) AS count_total_5years_to_14years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 60 AND 168 AND sex = 'male') AS count_total_5years_to_14years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign BETWEEN 60 AND 168 AND sex = 'female') AS count_total_5years_to_14years_female,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 180) AS count_total_over_15_years,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 180 AND sex = 'male') AS count_total_over_15_years_male,
    COUNT(DISTINCT uuid) FILTER (WHERE age_in_months_at_campaign > 180 AND sex = 'female') AS count_total_over_15_years_female
  FROM
    population_with_age_at_campaign
  GROUP BY
    cycle_name,
    start_date,
    county_name,
    sub_county_name,
    chu_name,
    chp_area_uuid,
    chp_area_name
)

SELECT
  cycle_name,
  county_name,
  sub_county_name,
  chu_name,
  chp_area_uuid,
  chp_area_name,
  start_date AS period_start,
  CONCAT(DATE_PART('year', start_date)::TEXT, LPAD(DATE_PART('month', start_date)::TEXT, 2, '0')) AS period,
  DATE_PART('epoch', start_date)::NUMERIC AS period_start_epoch,
  --{{ dbt_utils.generate_surrogate_key(['chu_code', 'chp_area_uuid', 'cycle_name']) }} AS moh_515_metrics_id,
  count_total_population,
  count_total_above_5_years,
  count_total_above_1_years,
  count_total_below_6months,
  count_total_below_6months_male,
  count_total_below_6months_female,
  count_total_btn_6months_and_7years,
  count_total_btn_6months_and_7years_male,
  count_total_btn_6months_and_7years_female,
  count_total_above_7years,
  count_total_above_7years_male,
  count_total_above_7years_female,
  count_total_1years_to_4years,
  count_total_1years_to_4years_male,
  count_total_1years_to_4years_female,
  count_total_2years_to_5years,
  count_total_2years_to_5years_male,
  count_total_2years_to_5years_female,
  count_total_5years_to_14years,
  count_total_5years_to_14years_male,
  count_total_5years_to_14years_female,
  count_total_over_15_years,
  count_total_over_15_years_male,
  count_total_over_15_years_female
FROM
  target_population_aggregated
ORDER BY
  county_name,
  sub_county_name,
  chu_name,
  cycle_name