{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county"], "unique": true},
      {"columns": ["category"]},
      {"columns": ["expected_households_per_month"]}
    ]
) }}

SELECT * FROM (VALUES
    -- Urban (1:100)
    ('Nairobi', 'urban', 100),
    ('Mombasa', 'urban', 100),
    ('Kisumu', 'urban', 100),
    ('Kiambu', 'urban', 100),
    ('Kisii', 'urban', 100),
    ('Vihiga', 'urban', 100),
    ('Nyamira', 'urban', 100),
    ('Muranga', 'urban', 100),
    ('Kakamega', 'urban', 100),
    ('Bungoma', 'urban', 100),

    -- Peri-Urban (1:50)
    ('Machakos', 'peri-urban', 50),
    ('Nakuru', 'peri-urban', 50),
    ('Uasin Gishu', 'peri-urban', 50),
    ('Kajiado', 'peri-urban', 50),
    ('Kericho', 'peri-urban', 50),
    ('Nyeri', 'peri-urban', 50),
    ('Embu', 'peri-urban', 50),
    ('Kirinyaga', 'peri-urban', 50),
    ('Trans Nzoia', 'peri-urban', 50),
    ('Laikipia', 'peri-urban', 50),
    ('Tharaka-Nithi', 'peri-urban', 50),
    ('Bomet', 'peri-urban', 50),
    ('Nandi', 'peri-urban', 50),
    ('Siaya', 'peri-urban', 50),
    ('Migori', 'peri-urban', 50),
    ('Homa Bay', 'peri-urban', 50),
    ('Busia', 'peri-urban', 50),
    ('Nyandarua', 'peri-urban', 50),
    ('Elgeyo-Marakwet', 'peri-urban', 50),
    ('Meru', 'peri-urban', 50),
    ('Makueni', 'peri-urban', 50),
    ('Kitui', 'peri-urban', 50),
    ('Taita-Taveta', 'peri-urban', 50),
    ('Kwale', 'peri-urban', 50),
    ('Kilifi', 'peri-urban', 50),

    -- ASAL (1:25)
    ('Turkana', 'asal', 25),
    ('Marsabit', 'asal', 25),
    ('Wajir', 'asal', 25),
    ('Mandera', 'asal', 25),
    ('Garissa', 'asal', 25),
    ('Isiolo', 'asal', 25),
    ('Samburu', 'asal', 25),
    ('Tana River', 'asal', 25),
    ('Lamu', 'asal', 25),
    ('West Pokot', 'asal', 25),
    ('Baringo', 'asal', 25),
    ('Narok', 'asal', 25)
) AS t(county, category, expected_households_per_month)
