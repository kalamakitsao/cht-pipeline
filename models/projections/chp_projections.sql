-- models/staging/expected_chps.sql
{{
  config(
    materialized='table',
    unique_key='county',
    tags=['reference', 'targets']
  )
}}

WITH county_targets AS (
  SELECT 
    county,
    expected_chps
  FROM (VALUES
    ('Baringo', 1920),
    ('Bomet', 2301),
    ('Bungoma', 3590),
    ('Busia', 2190),
    ('Elgeyomarakwet', 1260),
    ('Embu', 1563),
    ('Garissa', 2500),
    ('Homa Bay', 2974),
    ('Isiolo', 753),
    ('Kajiado', 1994),
    ('Kakamega', 4250),
    ('Kericho', 1699),
    ('Kiambu', 3488),
    ('Kilifi', 3870),
    ('Kirinyaga', 1221),
    ('Kisii', 2940),
    ('Kisumu', 2981),
    ('Kitui', 2470),
    ('Kwale', 1671),
    ('Laikipia', 1247),
    ('Lamu County', 529),
    ('Machakos', 2877),
    ('Makueni', 3600),
    ('Mandera', 1268),
    ('Marsabit', 1983),
    ('Meru', 3561),
    ('Migori', 2945),
    ('Mombasa', 2387),
    ('Murang''a', 2090), -- Note the escaped single quote
    ('Nairobi', 7586),
    ('Nakuru', 3690),
    ('Nandi', 1520),
    ('Narok', 2309),
    ('Nyamira', 1430),
    ('Nyandarua', 1413),
    ('Nyeri', 2563),
    ('Samburu', 1604),
    ('Siaya', 2128),
    ('Taita Taveta', 1322),
    ('Tana River', 940),
    ('Tharaka Nithi', 1265),
    ('Transnzoia', 2249),
    ('Turkana', 2558),
    ('Uasin Gishu', 2065),
    ('Vihiga County', 1447),
    ('Wajir', 1197),
    ('West Pokot', 2423),
    ('Kenya', 107831)
  ) AS t(county, expected_chps)
)

SELECT
  county,
  expected_chps,
  CURRENT_TIMESTAMP AS last_updated
FROM county_targets
WHERE county != 'Kenya' -- Exclude national total from main table