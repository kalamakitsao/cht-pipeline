WITH months AS (
    -- Generate last 12 full months up to current month
    SELECT date_trunc('month', CURRENT_DATE) - (n || ' months')::interval AS period_start
    FROM generate_series(0, 11) AS n
),

-- Active CHPs
-- Households visited
-- Population served
-- Total Referrals
-- NCDs screening
-- Child health illnesses