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
        -- Child Health: Assessments and ICCM
        ('u5_assessed', 'Total U5 Population Assessed', 'Child Health (<5 years)', 'Children under 5 assessed during the period', 'count', 'child_health'),
        ('u5_diarrhea_cases', 'U5 Diarrhea Cases', 'Child Health (<5 years)', 'Confirmed diarrhea cases in children under 5', 'count', 'child_health'),
        ('u5_pneumonia_cases', 'U5 Pneumonia Cases', 'Child Health (<5 years)', 'Suspected pneumonia cases in children under 5', 'count', 'child_health'),
        ('u5_malnutrition_cases', 'U5 Malnutrition Cases', 'Child Health (<5 years)', 'Suspected malnutrition cases in children under 5', 'count', 'child_health'),
        ('u5_malnutrition_male', 'Male U5 Malnutrition Cases', 'Child Health (<5 years)', 'Suspected malnutrition in male children under 5', 'count', 'child_health'),
        ('u5_malnutrition_female', 'Female U5 Malnutrition Cases', 'Child Health (<5 years)', 'Suspected malnutrition in female children under 5', 'count', 'child_health'),
        ('u5_tested_malaria', 'U5 Malaria Tests', 'Child Health (<5 years)', 'Malaria tests done on children under 5', 'count', 'child_health'),
        ('u5_confirmed_malaria_cases', 'Confirmed U5 Malaria Cases', 'Child Health (<5 years)', 'Confirmed malaria in children under 5', 'count', 'child_health'),
        ('u5_suspected_malaria_cases', 'Suspected U5 Malaria Cases', 'Child Health (<5 years)', 'Suspected malaria in children under 5', 'count', 'child_health'),
        ('u5_treated_malaria', 'Treated for Malaria', 'Child Health (<5 years)', 'Children treated for malaria', 'count', 'child_health'),
        ('u5_treated_pneumonia', 'Treated for Pneumonia', 'Child Health (<5 years)', 'Children treated for pneumonia', 'count', 'child_health'),
        ('u5_treated_diarrhoea', 'Treated for Diarrhoea', 'Child Health (<5 years)', 'Children treated for diarrhoea', 'count', 'child_health'),
        ('referred_for_malaria', 'Referred for Malaria', 'Child Health (<5 years)', 'Children referred for malaria treatment', 'count', 'child_health'),
        ('referred_for_pneumonia', 'Referred for Pneumonia', 'Child Health (<5 years)', 'Children referred for pneumonia treatment', 'count', 'child_health'),
        ('referred_for_malnutrition', 'Referred for Malnutrition', 'Child Health (<5 years)', 'Children referred for malnutrition', 'count', 'child_health'),
        ('referred_for_dirrhoea', 'Referred for Diarrhoea', 'Child Health (<5 years)', 'Children referred for diarrhoea treatment', 'count', 'child_health'),
        ('u5_referred', 'Under 5 Referred', 'Child Health (<5 years)', 'Children Referred for Further Assessment', 'count', 'child_health'),
        ('u5_referred_male', 'Under 5 Referred Male', 'Male Child Health (<5 years)', 'Children Referred for Further Assessment', 'count', 'child_health'),
        ('u5_referred_female', 'Under 5 Referred Female', 'Female Child Health (<5 years)', 'Children Referred for Further Assessment', 'count', 'child_health'),
        ('needs_deworming_follow_up_male', 'Male Children Needing Deworming', 'Child Health (<5 years)', 'Male children needing follow-up deworming', 'count', 'child_health'),
        ('needs_deworming_follow_up_female', 'Female Children Needing Deworming', 'Child Health (<5 years)', 'Female children needing follow-up deworming', 'count', 'child_health'),


        -- Pregnancy-related
        ('currently_pregnant', 'Current Pregnant Women', 'Maternal Health Services', 'Number of pregnant women within the period', 'count','maternal_health'),
        ('new_pregnancies', 'Newly Registered Pregnant Women', 'Maternal Health Services', 'Number of newly registered pregnant women within the period', 'count','maternal_health'),
        ('teen_pregnancies', 'Teen Pregnancies (10-19)', 'Maternal Health Services', 'Number of pregnant women aged 10-19 within the period', 'count','maternal_health'),
        ('new_teen_pregnancies', 'New Teen Pregnancies (10-19)', 'Maternal Health Services', 'Newly identified pregnancies among women aged 10–19', 'count','maternal_health'),
        ('pregnant_women_visited', 'Pregnant Women Visited', 'Maternal Health Services', 'Number of pregnant women visited during the period', 'count','maternal_health'),
        ('pregnant_women_referred', 'Pregnant Women Referred (Any Reason)', 'Maternal Health Services', 'Pregnant women referred during the period for any reason', 'count','maternal_health'),
        ('pregnant_women_referred_missed_anc', 'Pregnant Women Referred for Missed ANC', 'Maternal Health Services', 'Pregnant women referred due to missed ANC visit', 'count','maternal_health'),
        ('new_pregnant_women_referred_anc', 'New Pregnant Women Referred for ANC', 'Maternal Health Services', 'Newly identified pregnant women referred for ANC', 'count','maternal_health'),
        ('new_teen_pregnant_women_referred_anc', 'New Teen Pregnant Women Referred for ANC', 'Maternal Health Services', 'Newly identified teen pregnant women referred for ANC', 'count', 'maternal_health'),
        ('referred_for_delivery', 'Pregnant Women Referred for Delivery', 'Maternal Health Services', 'Women referred for facility delivery', 'count','maternal_health'),
        ('skilled_birth_attendance', 'Facility Based Deliveries', 'Maternal Health Services', 'Deliveries conducted by skilled providers at health facility', 'count','maternal_health'),
        ('first_trimester_pregnancies', 'First Trimester Pregnancies', 'Maternal Health Services', 'New pregnancies with gestational age under 12 weeks', 'count','maternal_health'),
        ('repeat_pregnancies', 'Repeat Pregnancies (<12 months)', 'Maternal Health Services', 'Pregnancies occurring within 12 months of last delivery', 'count','maternal_health'),
        ('actively_pregnant_women', 'Actively Pregnant Women (EDD-based)', 'Maternal Health Services', 'Estimated number of women actively pregnant during the period based on EDD window', 'count','maternal_health'),
       
        -- Newborn and PNC
        ('deliveries', 'Total Deliveries', 'Maternal Health Services', 'Number of new deliveries that took place', 'count', 'maternal_health'),
        ('newborns', 'Newborns', 'Maternal Health Services', 'Number of newborns born during the period', 'count', 'maternal_health'),
        ('referred_for_pnc', 'Newborn Referred for PNC Services', 'Child Health (<5 years)', 'Newborns referred for postnatal care follow-up', 'count', 'child_health'),
        ('newborns_needing_follow_up', 'Newborns Needing Follow-up', 'Child Health (<5 years)', 'Newborns needing follow-up care', 'count', 'child_health'),
        ('newborns_needing_follow_up_danger_signs', 'Newborns Needing Follow-up for Danger Signs', 'Child Health (<5 years)', 'Newborns needing follow-up for danger signs', 'count', 'child_health'),
        ('newborns_needing_follow_up_missed_visit', 'Newborns Needing Follow-up for Missed PNC Visit', 'Child Health (<5 years)', 'Newborns needing follow-up for missed postnatal visit', 'count', 'child_health'),
        ('pnc_visits_completed', 'Postnatal Care Visits Completed', 'Maternal Health Services', 'Mothers who completed all recommended postnatal visits', 'count', 'maternal_health'),
        ('needs_follow_up_danger_signs', 'Follow-up Needed for Danger Signs', 'Child Health (<5 years)', 'Newborns needing follow-up for danger signs', 'count', 'child_health'),
        ('needs_follow_up_missed_visit', 'Follow-up Needed for Missed PNC Visit', 'Child Health (<5 years)', 'Newborns needing follow-up for missed postnatal visit', 'count', 'child_health'),
    
        -- Immunization
        ('under_1_immunised', 'Under 1 Year Fully Immunised', 'Child Health (<5 years)', 'Children under 1 year who are fully immunised', 'count', 'child_health'),
        ('under_1_immunised_male', 'Male Under 1 Year Fully Immunised', 'Child Health (<5 years)', 'Male children under 1 year fully immunised', 'count', 'child_health'),
        ('under_1_immunised_female', 'Female Under 1 Year Fully Immunised', 'Child Health (<5 years)', 'Female children under 1 year fully immunised', 'count', 'child_health'),
        ('children_turning_one', 'Children Turning One', 'Child Health (<5 years)', 'Children turning 1 year during reporting period', 'count', 'child_health'),
        ('male_turning_one', 'Male Children Turning One', 'Child Health (<5 years)', 'Male children turning 1 year during period', 'count', 'child_health'),
        ('female_turning_one', 'Female Children Turning One', 'Child Health (<5 years)', 'Female children turning 1 year during period', 'count', 'child_health'),
        ('referred_immunization', 'Referred for Immunization Defaulting', 'Child Health (<5 years)', 'Children referred for defaulting on their immunization schedule', 'count', 'child_health'),
        ('referred_immunization_male', 'Boys Referred for Immunization Defaulting', 'Child Health (<5 years)', 'Male children referred for defaulting on immunization schedule', 'count', 'child_health'),
        ('referred_immunization_female', 'Girls Referred for Immunization Defaulting', 'Child Health (<5 years)', 'Female children referred for defaulting on immunization schedule', 'count', 'child_health'),
        ('referred_missed_vaccine', 'Referred for Missed Vaccine', 'Child Health (<5 years)', 'Children referred for missed vaccinations', 'count', 'child_health'),
        ('referred_missed_vaccine_male', 'Boys Referred – Missed Vaccine', 'Child Health (<5 years)', 'Male children referred due to missed vaccines', 'count', 'child_health'),
        ('referred_missed_vaccine_female', 'Girls Referred – Missed Vaccine', 'Child Health (<5 years)', 'Female children referred due to missed vaccines', 'count','child_health'),

        -- Developmental Screening
        ('referred_for_development_milestones', 'Referred for Development Milestones', 'Child Health (<5 years)', 'Children referred for milestone screening concerns', 'count', 'child_health'),
        ('male_referred_for_development_milestones', 'Male Referred for Development Milestones', 'Child Health (<5 years)', 'Male children referred for milestone screening concerns', 'count', 'child_health'),
        ('female_referred_for_development_milestones', 'Female Referred for Development Milestones', 'Child Health (<5 years)', 'Female children referred for milestone screening concerns', 'count', 'child_health'),
        ('referred_for_developmental_delays', 'Referred for Developmental Delays', 'Child Health (<5 years)', 'Children referred for developmental delays', 'count', 'child_health'),

        -- Growth Monitoring
        ('referred_growth_monitoring_male', 'Boys Referred for Growth Monitoring', 'Child Health (<5 years)', 'Male children referred for growth monitoring', 'count', 'child_health'),
        ('referred_growth_monitoring_female', 'Girls Referred for Growth Monitoring', 'Child Health (<5 years)', 'Female children referred for growth monitoring', 'count', 'child_health'),

        -- Over 5 and NCD Metrics
        ('over_5_assessments', 'Over 5 Assessments', 'NCDs (Chronic Illnesses)', 'Assessments done for over 5 population', 'count','ncds'),
        ('over_5_referred', 'Over 5 Referred', 'NCDs (Chronic Illnesses)', 'Referrals for population over 5', 'count', 'ncds'),
        ('over_5_referred_male', 'Male Over 5 Referrals', 'NCDs (Chronic Illnesses)', 'Over Male 5s assessed and referred', 'count', 'ncds'),
        ('over_5_referred_female', 'Female Over 5 Referrals', 'NCDs (Chronic Illnesses)', 'Over 5s Female assessed and referred', 'count', 'ncds'),
        ('screened_diabetes', 'Screened for Diabetes', 'NCDs (Chronic Illnesses)', 'Individuals screened for diabetes', 'count','ncds'),
        ('screened_diabetes_male', 'Male Screened for Diabetes', 'NCDs (Chronic Illnesses)', 'Males screened for diabetes', 'count','ncds'),
        ('screened_diabetes_female', 'Female Screened for Diabetes', 'NCDs (Chronic Illnesses)', 'Females screened for diabetes', 'count', 'ncds'),
        ('referred_diabetes', 'Referred for Diabetes', 'NCDs (Chronic Illnesses)', 'Individuals referred for diabetes care', 'count', 'ncds'),
        ('screened_hypertension', 'Screened for Hypertension', 'NCDs (Chronic Illnesses)', 'Individuals screened for hypertension', 'count','ncds'),
        ('referred_hypertension', 'Referred for Hypertension', 'NCDs (Chronic Illnesses)', 'Individuals referred for hypertension care', 'count','ncds'),
        ('screened_mental_health', 'Screened for Mental Health', 'NCDs (Chronic Illnesses)', 'Screened for mental health issues', 'count','ncds'),
        ('referred_mental_health', 'Referred for Mental Health', 'NCDs (Chronic Illnesses)', 'Referrals for mental health support', 'count','ncds'),
        ('referred_diabetes_male', 'Male Referred for Diabetes', 'NCDs (Chronic Illnesses)', 'Number of male referred for diabetes care', 'count','ncds'),
        ('referred_diabetes_female', 'Female Referred for Diabetes', 'NCDs (Chronic Illnesses)', 'Number of female referred for diabetes care', 'count','ncds'),
        ('screened_hypertension_male', 'Male Screened for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of male screened for hypertension', 'count','ncds'),
        ('screened_hypertension_female', 'Female Screened for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of female screened for hypertension', 'count','ncds'),
        ('referred_hypertension_male', 'Male Referred for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of Male referred for hypertension care', 'count','ncds'),
        ('referred_hypertension_female', 'Female Referred for Hypertension', 'NCDs (Chronic Illnesses)', 'Number of Female referred for hypertension care', 'count','ncds'),
        ('screened_mental_health_male', 'Male Screened for Mental Health', 'NCDs (Chronic Illnesses)', 'Number of male screened for Mental Health', 'count','ncds'),
        ('screened_mental_health_female', 'Female Screened for Mental Health', 'NCDs (Chronic Illnesses)', 'Number of female screened for Mental Health', 'count','ncds'),
        ('referred_mental_health_male', 'Male Referred for Mental Health', 'NCDs (Chronic Illnesses)', 'Number of Male referred for Mental Health care', 'count','ncds'),
        ('referred_mental_health_female', 'Female Referred for Mental Health', 'NCDs (Chronic Illnesses)', 'Number of Female referred for Mental Health care', 'count','ncds'),
        -- Household & Population Registration
        ('households_registered', 'Households Registered', 'Households and Population Registration', 'Number of households registered in eCHIS', 'count','household_registration'),
        ('population', 'Total Population', 'Households and Population Registration', 'Individuals registered in eCHIS', 'count','household_registration'),
        ('population_male', 'Male Population', 'Households and Population Registration', 'Male individuals registered in eCHIS', 'count','household_registration'),
        ('population_female', 'Female Population', 'Households and Population Registration', 'Female individuals registered in eCHIS', 'count','household_registration'),
        ('under_5_population', 'Under 5 Population', 'Child Health (<5 years)', 'Total number of individuals registered in eCHIS, currently under 5 years', 'count','child_health'),
        ('population_under_5_male', 'Population Under 5 Male', 'Child Health (<5 years)', 'Total number of Male registered in eCHIS, currently under 5 years', 'count', 'child_health'),
        ('population_under_5_female', 'Population Under 5 Female', 'Child Health (<5 years)', 'Total number of Female registered in eCHIS, currently under 5 years', 'count','child_health'),
        ('exp_population', 'Expected Population Registration', 'Households and Population Registration', 'Expected vs registered population', 'count', 'household_registration'),
        ('perc_hh_registered', 'Household Registration Rate', 'Households and Population Registration', 'Proportion of HHs registered per county', 'count','household_registration'),
        ('perc_hh_with_insurance', 'HHs with Insurance', 'Households and Population Registration', 'Proportion of households with active insurance', 'count','household_registration'),

        -- CHP Service Delivery & Performance
        ('hh_visited', 'Households Visited', 'CHP Service Delivery', 'Households visited by CHPs for services', 'count','chp_service_delivery'),
        ('people_served', 'People Served', 'CHP Service Delivery', 'Individuals receiving services during the period', 'count', 'chp_service_delivery'),
        ('monthly_cu_meetings', 'Monthly CU Meetings', 'CHP Service Delivery', 'CHPs attending mandatory monthly meetings', 'count', 'chp_service_delivery'),
        ('other_community_events', 'Community Events Participation', 'CHP Service Delivery', 'CHP participation in community events', 'count', 'chp_service_delivery'),
        ('revised_active_chps', 'Revised Active CHPs', 'CHP Service Delivery', 'CHPs scoring 80%+ across KPIs (visits, referrals etc.)', 'count', 'chp_service_delivery'),
        ('chps_enrolled', 'CHPs Enrolled in eCHIS', 'CHP Service Delivery', 'CHPs with eCHIS credentials and log-ins', 'count', 'chp_service_delivery'),
        ('chps_with_hholds', 'CHPs Who Registered Households', 'CHP Service Delivery', 'CHPs that enrolled at least one household', 'count', 'chp_service_delivery'),
        ('perc_active_chps', 'Proportion of Active CHPs', 'CHP Service Delivery', 'Percentage of CHPs actively providing services', 'count', 'chp_service_delivery'),
        ('active_chps_new', 'CHPs Who Visited their requiered Household Target', 'CHP Service Delivery', 'CHPs who met their household visit targets', 'count', 'chp_service_delivery'),
        ('chps_expected', 'CHPs expected to be working', 'CHP Service Delivery', 'CHPs who are expected to be reporting on eCHIS', 'count', 'chp_service_delivery'),

        -- Referral & Death Reporting
        ('total_referrals', 'Total Referrals', 'Referrals', 'Individuals referred by CHPs across all age groups', 'count', 'referrals'),
        ('maternal_deaths', 'Maternal Deaths', 'Birth and Death Reporting', 'Deaths of women during pregnancy or within 42 days post-delivery', 'count','births_death_reporting'),
        ('neonatal_deaths', 'Neonatal Deaths', 'Birth and Death Reporting', 'Deaths of children within 28 days of birth', 'count','births_death_reporting'),
        ('child_deaths', 'Child Deaths (29d-5y)', 'Birth and Death Reporting', 'Deaths of children aged 29 days to 5 years', 'count','births_death_reporting'),
        ('child_deaths_female', 'Child Deaths (29d-5y) - Female', 'Birth and Death Reporting', 'Deaths of female children aged 29 days to 5 years', 'count','births_death_reporting'),
        ('child_deaths_male', 'Child Deaths (29d-5y) - Male', 'Birth and Death Reporting', 'Deaths of male children aged 29 days to 5 years', 'count','births_death_reporting'),
        ('over_5_deaths_female', 'Over 5 deaths  - Male', 'Birth and Death Reporting', 'Deaths of male children aged 29 days to 5 years', 'count','births_death_reporting'),
        ('over_5_deaths_male', 'Over 5 deaths - Male', 'Birth and Death Reporting', 'Deaths of male children aged 29 days to 5 years', 'count','births_death_reporting'),
        ('total_deaths', 'Total Deaths Reported', 'Birth and Death Reporting', 'Total deaths reported by CHPs', 'count','births_death_reporting'),


        -- SHA
        ('households_with_active_insurance', 'Households With Active Insurance Cover', 'Households and Population Registration', 'Number of households that have an active insurance cover', 'count','household_registration'),
        ('households_registered_on_sha', 'Households Registered on SHA', 'Households and Population Registration', 'Number of households registered with NHIF, SHIF, or SHA schemes', 'count','household_registration'),
        ('households_assessed_sha', 'Households Assessed for SHA Registration', 'Households and Population Registration', 'Number of households assessed for SHA registration (true or false)', 'count','household_registration'),
        ('households_with_sha', 'Households Registered for SHA', 'Households and Population Registration', 'Number of households with at least one member confirmed as registered to SHA', 'count','household_registration'),

        -- Additional metrics to be included in maps
        ('rate_teen_pregnancy',          'Teen pregnancies rates',        'Maternal Health Services',  'Proportion of registered pregnancies that are of girls aged between 10-19 years', 'proportion', 'maternal_health'),
        ('rate_maternal_death',          'Maternal death rates',          'Maternal Health Services',  'Maternal mortality rate for every 100,000 births registered',                     'float', 'maternal_health'),
        ('rate_malnutrition_referral',   'Malnutrition rates by county',  'Child Health (<5 years)',   'Proportion of screened children referral for suspected malnutrition',             'proportion', 'child_health'),
        ('rate_pneumonia_referral',      'Pneumonia referral rates',      'Child Health (<5 years)',   'Proportion of screened children referral for suspected pneumonia',                'proportion', 'child_health'),
        ('rate_malaria_referral',        'Malaria referral rates',        'Child Health (<5 years)',   'Proportion of screened children referral for suspected malaria',                  'proportion', 'child_health'),
        ('rate_diarrhoea_referral',      'Diarrhoea referral rates',      'Child Health (<5 years)',   'Proportion of screened children referral for suspected diarrhoea',                'proportion', 'child_health'),
        ('rate_mental_health_referral',  'Mental Health referral rates',  'NCDs (Chronic Illnesses)',  'Proportion of people screened for mental health that were referred for further assessment', 'proportion', 'ncds'),
        ('rate_hypertension_referral',   'Hypertension referral rates',   'NCDs (Chronic Illnesses)',  'Proportion of people screened for hypertension that were referred for further assessment', 'proportion', 'ncds'),
        ('rate_diabetes_referral',       'Diabetes referral rates',       'NCDs (Chronic Illnesses)',  'Proportion of people screened for diabetes that were referred for further assessment', 'proportion', 'ncds'),
        ('rate_active_chp',              'Active CHPs',                   'CHP Service Delivery',      'CHPs who met the performance target',                                             'proportion', 'chp_service_delivery'),
        ('rate_echis_pop_coverage',      'Registered Population on eCHIS','CHP Service Delivery',      'Proportion of projected population registered by CHPs',                           'proportion', 'chp_service_delivery'),
        ('rate_household_visit',         'Households visit rate',         'CHP Service Delivery',      'Household visit coverage rates by CHPs',                                          'proportion', 'chp_service_delivery'),
        ('monthly_rates_active_chp',     'Monthly Active CHPs',           'CHP Service Delivery',      'Monthly active CHP trends',                                                       'proportion', 'chp_service_delivery'),
        ('monthly_rates_pop_coverage',   'Monthly People served',         'CHP Service Delivery',      'Monthly people served coverage rates',                                            'proportion', 'chp_service_delivery'),
        ('monthly_rates_household_visit','Monthly Households visited',    'CHP Service Delivery',      'Monthly household visit coverage rates',                                          'proportion', 'chp_service_delivery')


) AS t(metric_id, name, group_name, description, unit, metric_group_id)
