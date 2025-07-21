-- models/smc_campaign_aggregates.sql

{{
  config(
    materialized='incremental',
    unique_key='chu_uuid',
    on_schema_change='append_new_columns',
    tags=['smc', 'campaigns']
  )
}}

WITH cycle_dates AS (
    SELECT 
        cycle_name,
        start_date,
        end_date
    FROM {{ ref('smc_cycle_dates') }}
),

campaigns_with_cycle AS (
    SELECT
        c.*,
        cd.cycle_name
    FROM {{ ref('campaign_service_smc') }} c
    JOIN cycle_dates cd 
        ON c.reported::date BETWEEN cd.start_date::date AND cd.end_date::date
),

campaigns AS (
    SELECT
        chp.uuid AS chp_area_id,
        chp.county_name AS county_name,
        chp.sub_county_name AS sub_county_name,
        chp.chu_name AS community_health_unit_name,
        chp.chu_uuid,
        cwc.cycle_name AS cycle,
        cwc.reported::date AS campaign_date,
        p.uuid,
        cwc.reported_by,
        cwc.patient_id,
        p.sex,
        cwc.patient_age_in_months,
        cwc.smc_treatment_given,
        cwc.redose,
        cwc.fever_status,
        cwc.smc_danger_signs,
        cwc.caregiver_consent,
        cwc.spaq_allergy,
        cwc.taken_al,
        cwc.cotrimazole,
        cwc.calc_pink_spaq::int AS pink_spaq,
        cwc.calc_green_spaq::int AS green_spaq
    FROM campaigns_with_cycle cwc
    JOIN {{ ref('patient_f_client') }} p ON p.uuid = cwc.patient_id
    JOIN {{ ref('household') }} hh ON p.household_id = hh.uuid
    JOIN {{ ref('chp_hierarchy') }} chp ON hh.chv_area_id = chp.uuid
)

SELECT
    county_name,
    sub_county_name,
    community_health_unit_name,
    chu_uuid,
    cycle,
    campaign_date,

    -- Reporting metrics
    COUNT(DISTINCT reported_by) AS chps_reporting,
    COUNT(uuid) AS campaign_forms_submitted,
    COUNT(DISTINCT patient_id) AS children_reached,

    -- Reach by age/sex
    SUM(CASE WHEN sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS reach_f_3_11m,
    SUM(CASE WHEN sex = 'male' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS reach_m_3_11m,
    SUM(CASE WHEN sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS reach_f_12_59m,
    SUM(CASE WHEN sex = 'male' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS reach_m_12_59m,

    -- Treatment metrics
    SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' THEN 1 ELSE 0 END) AS female_children_treated,
    SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS treated_f_3_11,
    SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'male' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS treated_m_3_11,
    SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS treated_f_12_59,
    SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'male' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS treated_m_12_59,

    -- Redosing metrics
    SUM(CASE WHEN redose = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS redosed_f_3_11m,
    SUM(CASE WHEN redose = 'yes' AND sex = 'male' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS redosed_m_3_11m,
    SUM(CASE WHEN redose = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS redosed_f_12_59m,
    SUM(CASE WHEN redose = 'yes' AND sex = 'male' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS redosed_m_12_59m,

    -- Referral metrics
    SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS referred_f_3_11m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'male' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS referred_m_3_11m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS referred_f_12_59m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'male' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS referred_m_12_59m,

    -- Exclusion metrics
    SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS excluded_f_3_11m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'male' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS excluded_m_3_11m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS excluded_f_12_59m,
    SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'male' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS excluded_m_12_59m,

    -- Treatment refusal reasons
    SUM(CASE WHEN smc_treatment_given = 'no' AND caregiver_consent = 'no' THEN 1 ELSE 0 END) AS no_treatment_no_consent,
    SUM(CASE WHEN smc_treatment_given = 'no' AND smc_danger_signs = 'yes' THEN 1 ELSE 0 END) AS no_treatment_danger_signs,
    SUM(CASE WHEN smc_treatment_given = 'no' AND spaq_allergy = 'yes' THEN 1 ELSE 0 END) AS no_treatment_spaq_allergy,
    SUM(CASE WHEN smc_treatment_given = 'no' AND taken_al = 'yes' THEN 1 ELSE 0 END) AS no_treatment_taken_al,
    SUM(CASE WHEN smc_treatment_given = 'no' AND fever_status = 'yes' THEN 1 ELSE 0 END) AS no_treatment_fever,
    SUM(CASE WHEN smc_treatment_given = 'no' AND cotrimazole = 'yes' THEN 1 ELSE 0 END) AS no_treatment_cotrimazole,

    -- Medication usage
    SUM(pink_spaq) AS pink_blister_packs_used,
    SUM(green_spaq) AS green_blister_packs_used,

    -- Calculated metrics
    COUNT(DISTINCT patient_id)::float / NULLIF(COUNT(DISTINCT reported_by), 0) AS avg_children_per_chp,
    SUM(CASE WHEN smc_treatment_given = 'yes' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(DISTINCT patient_id), 0) AS treatment_coverage_rate
     --{{ dbt_utils.generate_surrogate_key(['cs.chp_area_id', 'cs.campaign_date']) }} AS snc_activity_summary_id
FROM campaigns cs
GROUP BY 
    county_name, 
    sub_county_name,
    community_health_unit_name,
    chu_uuid,
    cycle,
    campaign_date