-- models/dashboards/ntd_campaign_metrics.sql

{{ config(
    materialized='incremental',
    unique_key='ntd_activity_id',
    on_schema_change='append_new_columns'
) }}

{% set form_indexes = [
    {'columns': ['uuid'], 'unique': true},
    {'columns': ['saved_timestamp']},
    {'columns': ['reported']},
    {'columns': ['chp_area_id']},
    {'columns': ['source_id']}
] %}
WITH campaign_service AS (
    SELECT
        cs_raw.chp_area_id,
        cs_raw.reported::date AS reported_date,
        cs_raw.chu_name,
        cs_raw.facility_name,
        ch.county as county_name,
        ch.sub_county as sub_county_name,
        SUM(COALESCE(praziquantel_quantity_issued, 0)) AS count_praziquantel_tablets_given,
        SUM(COALESCE(total_mebendazole_tabs_count, 0)) AS count_mebendazole_tablets_given,
        SUM(COALESCE(total_albendazole_tabs_count, 0)) AS count_albendazole_tablets_given,
        -- Treated with Mebendazole
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 12 AND 59 AND treated_with_mebendazole IS TRUE ) AS count_total_1years_to_4years_male_treated_with_mebe,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 12 AND 59 AND treated_with_mebendazole IS TRUE ) AS count_total_1years_to_4years_female_treated_with_mebe,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_mebendazole IS TRUE ) AS count_total_5years_to_15years_male_treated_with_mebe,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_mebendazole IS TRUE ) AS count_total_5years_to_15years_female_treated_with_mebe,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int > 180 AND treated_with_mebendazole IS TRUE ) AS count_total_15_plus_years_female_treated_with_mebe,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int > 180 AND treated_with_mebendazole IS TRUE ) AS count_total_15_plus_years_male_treated_with_mebe,
        -- Treated with Albendazole
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 12 AND 59 AND treated_with_albendazole IS TRUE ) AS count_total_1years_to_4years_female_treated_with_albe,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 12 AND 59 AND treated_with_albendazole IS TRUE ) AS count_total_1years_to_4years_male_treated_with_albe,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_albendazole IS TRUE ) AS count_total_5years_to_15years_female_treated_with_albe,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_albendazole IS TRUE ) AS count_total_5years_to_15years_male_treated_with_albe,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int  > 180  AND treated_with_albendazole IS TRUE ) AS count_total_15_plus_years_female_treated_with_albe,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int  > 180  AND treated_with_albendazole IS TRUE ) AS count_total_15_plus_years_male_treated_with_albe,
        -- Treated with Praziquantel
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 24 AND 59 AND treated_with_praziquantel IS TRUE) AS count_total_2years_to_4years_male_treated_with_prazi,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 24 AND 59 AND treated_with_praziquantel IS TRUE) AS count_total_2years_to_4years_female_treated_with_prazi,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_praziquantel IS TRUE) AS count_total_5years_to_14years_male_treated_with_prazi,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int BETWEEN 60 AND 179 AND treated_with_praziquantel IS TRUE) AS count_total_5years_to_14years_female_treated_with_prazi,
        COUNT(*) FILTER (WHERE patient_gender = 'male' AND age_in_months::int  > 180  AND treated_with_praziquantel IS TRUE) AS count_total_15_plus_years_male_treated_with_prazi,
        COUNT(*) FILTER (WHERE patient_gender = 'female' AND age_in_months::int  > 180  AND treated_with_praziquantel IS TRUE) AS count_total_15_plus_years_female_treated_with_prazi,
        -- Overall treatment counts
        COUNT(*) FILTER (WHERE treated_with_mebendazole) AS count_total_treated_with_mebe,
        COUNT(*) FILTER (WHERE treated_with_albendazole) AS count_total_treated_with_albe,
        COUNT(*) FILTER (WHERE treated_with_praziquantel) AS count_total_with_prazi
    FROM {{ ref('campaign_service_ntd') }} cs_raw
    JOIN v1.'mv_location_hierarchy' ch ON cs_raw.chp_area_id = ch.uuid
    GROUP BY
        cs_raw.chp_area_id,
        cs_raw.reported::date,
        cs_raw.chu_name,
        cs_raw.facility_name,
        ch.county,
        ch.sub_county
),
campaign_cycles AS (
    -- Your campaign_cycles model
    SELECT
        campaign_name,
        cycle_name,
        start_date,
        end_date,
        target_counties
    FROM {{ ref('ntd_campaign_dates') }}
)

SELECT
    cs.chp_area_id,
    cs.reported_date,
    cs.county_name,
    cs.sub_county_name,
    cs.chu_name,
    cs.facility_name,
    cs.count_praziquantel_tablets_given,
    cs.count_mebendazole_tablets_given,
    cs.count_albendazole_tablets_given,
    -- Mebendazole treatment counts
    count_total_1years_to_4years_male_treated_with_mebe,
    count_total_1years_to_4years_female_treated_with_mebe,
    count_total_5years_to_15years_male_treated_with_mebe,
    count_total_5years_to_15years_female_treated_with_mebe,
    count_total_15_plus_years_female_treated_with_mebe,
    count_total_15_plus_years_male_treated_with_mebe,
    -- Albendazole treatment counts
    count_total_1years_to_4years_female_treated_with_albe,
    count_total_1years_to_4years_male_treated_with_albe,
    count_total_5years_to_15years_female_treated_with_albe,
    count_total_5years_to_15years_male_treated_with_albe
    count_total_15_plus_years_female_treated_with_albe,
    count_total_15_plus_years_male_treated_with_albe,
    -- Praziquantel treatment counts
    count_total_2years_to_4years_male_treated_with_prazi,
    count_total_2years_to_4years_female_treated_with_prazi,
    count_total_5years_to_14years_male_treated_with_prazi,
    count_total_5years_to_14years_female_treated_with_prazi,
    count_total_15_plus_years_male_treated_with_prazi,
    count_total_15_plus_years_female_treated_with_prazi,
    -- Overall treatment counts
    cs.count_total_treated_with_mebe,
    cs.count_total_treated_with_albe,
    cs.count_total_with_prazi,
    -- Campaign cycle information
    cc.start_date,
    cc.end_date,
    cc.campaign_name, 
    cc.cycle_name,    
    {{ dbt_utils.generate_surrogate_key(['cs.chp_area_id', 'cs.reported_date']) }} AS ntd_activity_id
FROM campaign_service cs
LEFT JOIN campaign_cycles cc ON
    cs.reported_date::date BETWEEN cc.start_date::date AND cc.end_date::date
    -- AND cs.county_name ILIKE '%' || target_county || '%'
    -- cs.county_name = ANY(cc.target_counties)