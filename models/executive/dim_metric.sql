{{ config(
    materialized='table',
    tags=["dimension", "static"],
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_metric_metric_id ON {{ this }} (metric_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_metric_group_name ON {{ this }} (group_name)",
        "CREATE INDEX IF NOT EXISTS idx_dim_metric_name ON {{ this }} (name)"
    ]
) }}

SELECT *
FROM (
    VALUES
        ('under_1_immunised', 'Under 1 Year Fully Immunised', 'Child Health (<5 years)', 'Number of children under 1 year who are fully immunised', 'count'),
        ('u5_assessed', 'Total U5 Population Assessed', 'Child Health (<5 years)', 'Total number of children under 5 years assessed during the period', 'count'),
        ('u5_diarrhea_cases', 'U5 Disease Burden – Diarrhea', 'Child Health (<5 years)', 'Confirmed cases of diarrhea in children under 5', 'count'),
        ('u5_pneumonia_cases', 'U5 Disease Burden – Pneumonia', 'Child Health (<5 years)', 'Confirmed cases of pneumonia in children under 5', 'count'),
        ('u5_malnutrition_cases', 'U5 Disease Burden – Malnutrition', 'Child Health (<5 years)', 'Confirmed cases of malnutrition in children under 5', 'count'),
        ('u5_malaria_cases', 'U5 Disease Burden – Malaria', 'Child Health (<5 years)', 'Confirmed cases of malaria in children under 5', 'count'),
        ('screened_diabetes', 'Screened for Diabetes', 'NCDs (Chronic Illnesses)', 'Number of people screened for diabetes', 'count'),
        ('referred_diabetes', 'Referred for Diabetes', 'NCDs (Chronic Illnesses)', 'Number of people referred for diabetes care', 'count'),
        ('screened_hypertension', 'Screened for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of people screened for hypertension', 'count'),
        ('referred_hypertension', 'Referred for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of people referred for hypertension care', 'count'),
        ('currently_pregnant', 'Current Pregnant Women', 'Maternal Health Services', 'Number of pregnant women within the period', 'count'),
        ('teen_pregnancies', 'Teen Pregnancies (10-19)', 'Maternal Health Services', 'Number of pregnant women aged 10-19 within the period', 'count'),
        ('pregnant_women_referred', 'Pregnant Women Referred', 'Maternal Health Services', 'Number of pregnant women referred within the period', 'count'),
        ('skilled_birth_attendance', 'Facility Based Deliveries', 'Maternal Health Services', 'Deliveries conducted by skilled health providers at the health facility', 'count'),
        ('chps_enrolled', 'Total number of CHPs enrolled in eCHIS', 'Active CHPs', 'CHPs who have been issued with eCHIS credentials and log in', 'count'),
        ('chps_with_hholds', 'Total number of CHPs who enrolled a household', 'Active CHPs', 'Any CHP who has registered a household', 'count'),
        ('perc_active_chps', 'Proportion CHPs Actively Providing Services', 'Active CHPs', 'Any CHP who has rendered a service', 'count'),
        ('households_registered', 'Total number of HHs', 'Households and Population registration', 'Total number of household registered in eCHIS', 'count'),
        ('population', 'Population', 'Households and Population registration', 'Total number of individuals registered in eCHIS', 'count'),
        ('exp_population', 'Expected Population registration', 'Households and Population registration', 'Total number of individuals registered in eCHIS as a proportion of expected population', 'count'),
        ('perc_hh_registered', 'Proportion of HHs registered per county', 'Households and Population registration', 'Proportion of HH registered per county as per annual KNBS projected HH numbers', 'count'),
        ('perc_hh_with_insurance', '% of HH with active insurance cover', 'Households and Population registration', 'Number of households', 'count'),
        ('hh_visited', 'Households visited', 'CHP SERVICE DELIVERY', 'HHs visited by the CHPs for service provision all time', 'count'),
        ('people_served', 'Persons Served', 'CHP SERVICE DELIVERY', 'Any unique person with a service form submitted within the period', 'count'),
        ('u5_pop_served', 'U5 population assessed', 'CHP SERVICE DELIVERY', 'Proportion of U5 population assessed in the period', 'count'),
        ('under_5_population', 'Under 5 Population', 'Child Health (<5 years)', 'Total number of individuals registered in eCHIS, currently under 5 years', 'count'),
        ('household_visits', 'Households Visited', 'Households and Population Registration', 'Number of unique households visited by CHPs during the reporting period', 'count'),
        ('active_chps', 'Active CHPs', 'Households and Population Registration', 'Number of Community Health Providers (CHPs) who conducted at least one household visit during the reporting period', 'count'),
        ('households_with_active_insurance', 'Households With Active Insurance Cover', 'Households and Population Registration', 'Number of households that have an active insurance cover', 'count'),
        ('households_registered_on_sha', 'Households Registered on SHA', 'Households and Population Registration', 'Number of households registered with NHIF, SHIF, or SHA schemes', 'count'),
        ('households_assessed_sha', 'Households Assessed for SHA Registration', 'Households and Population Registration', 'Number of households assessed for SHA registration (true or false)', 'count'),
        ('households_with_sha', 'Households Registered for SHA', 'Households and Population Registration', 'Number of households with at least one member confirmed as registered to SHA', 'count'),
        ('deliveries', 'Total Deliveries', 'Maternal Health Services', 'Number of New deliveries that took place', 'count'),
        ('total_referrals', 'Total Referrals', 'Referrals', 'Total number of distinct individuals referred by CHPs across all age groups per period.', 'count'),
        ('maternal_deaths', 'Maternal Deaths', 'Birth and Death Reporting', 'Any death reported of a woman within 42 days of delivery or currently pregnant.', 'count'),
        ('neonatal_deaths', 'Neonatal Deaths', 'Birth and Death Reporting', 'Any death reported of a child within 28 days of birth.', 'count'),
        ('child_deaths', 'Child Deaths', 'Birth and Death Reporting', 'Any death reported of a child aged between 29 days and 5 years.', 'count'),
        ('total_deaths', 'Total Deaths', 'Birth and Death Reporting', 'Total number of death reports submitted by CHPs.', 'count'),
        ('over_5_assessments', 'Over 5 Assessments', 'NCDs (Chronic Illnesses)', 'Number of assessments on over 5s', 'count')
) AS t (metric_id, name, group_name, description, unit)
