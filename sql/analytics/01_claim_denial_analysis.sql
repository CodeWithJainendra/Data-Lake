-- =====================================================================
-- CLAIM DENIAL ANALYSIS
-- The #1 metric every Revenue Cycle / CDI team tracks: which claims are
-- being denied, by which payers, for which reasons, and how much money
-- is at risk.
-- Run in: Trino (catalog: hive, schema: curated)
-- =====================================================================

-- 1. Top denial reasons (by frequency AND dollars at risk)
SELECT
    denial_reason_code,
    COUNT(*)                                    AS denial_count,
    ROUND(SUM(billed_amount), 2)                AS total_billed_at_risk,
    ROUND(AVG(billed_amount), 2)                AS avg_claim_value,
    ROUND(SUM(billed_amount) * 100.0 /
          SUM(SUM(billed_amount)) OVER (), 2)   AS pct_of_total_denied_dollars
FROM hive.curated.fact_claims
WHERE is_denied = TRUE
GROUP BY denial_reason_code
ORDER BY total_billed_at_risk DESC;


-- 2. Denial rate by payer (which payers are denying us the most?)
SELECT
    payer_name,
    payer_type,
    COUNT(*)                                            AS total_claims,
    SUM(CASE WHEN is_denied THEN 1 ELSE 0 END)          AS denied_claims,
    ROUND(SUM(CASE WHEN is_denied THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                        AS denial_rate_pct,
    ROUND(SUM(CASE WHEN is_denied THEN billed_amount ELSE 0 END), 2)
                                                        AS denied_dollars
FROM hive.curated.fact_claims
GROUP BY payer_name, payer_type
ORDER BY denial_rate_pct DESC;


-- 3. Denial trend over time (monthly)
SELECT
    submitted_year,
    submitted_month,
    COUNT(*)                                            AS total_claims,
    SUM(CASE WHEN is_denied THEN 1 ELSE 0 END)          AS denied_claims,
    ROUND(SUM(CASE WHEN is_denied THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                        AS denial_rate_pct
FROM hive.curated.fact_claims
GROUP BY submitted_year, submitted_month
ORDER BY submitted_year, submitted_month;


-- 4. Denials by department (which service lines are at risk?)
SELECT
    department,
    COUNT(*)                                            AS total_claims,
    SUM(CASE WHEN is_denied THEN 1 ELSE 0 END)          AS denied_claims,
    ROUND(SUM(CASE WHEN is_denied THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                        AS denial_rate_pct,
    ROUND(SUM(CASE WHEN is_denied THEN billed_amount ELSE 0 END), 2)
                                                        AS denied_dollars
FROM hive.curated.fact_claims
WHERE department IS NOT NULL
GROUP BY department
ORDER BY denied_dollars DESC;
