-- =====================================================================
-- REVENUE CYCLE ANALYSIS
-- Track billed vs collected, days-in-AR, and collection rate trends.
-- =====================================================================

-- 1. Revenue funnel: Billed → Allowed → Paid (and where the leakage is)
SELECT
    ROUND(SUM(billed_amount),     2) AS total_billed,
    ROUND(SUM(allowed_amount),    2) AS total_allowed,
    ROUND(SUM(paid_amount),       2) AS total_paid,
    ROUND(SUM(adjustment_amount), 2) AS total_adjustments,
    ROUND(SUM(paid_amount) * 100.0 / NULLIF(SUM(billed_amount), 0), 2)
                                     AS collection_rate_pct
FROM hive.curated.fact_claims;


-- 2. Monthly revenue trend (last 24 months)
SELECT
    submitted_year,
    submitted_month,
    COUNT(*)                          AS claim_count,
    ROUND(SUM(billed_amount), 2)      AS billed,
    ROUND(SUM(paid_amount), 2)        AS paid,
    ROUND(SUM(paid_amount) * 100.0 / NULLIF(SUM(billed_amount), 0), 2)
                                      AS collection_rate_pct,
    ROUND(AVG(days_to_adjudicate), 1) AS avg_days_to_adjudicate
FROM hive.curated.fact_claims
GROUP BY submitted_year, submitted_month
ORDER BY submitted_year DESC, submitted_month DESC
LIMIT 24;


-- 3. Top 10 highest-billing departments
SELECT
    department,
    COUNT(*)                       AS claims,
    ROUND(SUM(billed_amount), 2)   AS billed,
    ROUND(SUM(paid_amount), 2)     AS paid,
    ROUND(AVG(collection_rate)*100, 2) AS avg_collection_pct
FROM hive.curated.fact_claims
WHERE department IS NOT NULL
GROUP BY department
ORDER BY billed DESC
LIMIT 10;


-- 4. Payer mix (% of revenue by payer)
SELECT
    payer_name,
    payer_type,
    ROUND(SUM(paid_amount), 2)            AS total_paid,
    ROUND(SUM(paid_amount) * 100.0 /
          SUM(SUM(paid_amount)) OVER (), 2) AS pct_of_revenue
FROM hive.curated.fact_claims
WHERE is_paid = TRUE
GROUP BY payer_name, payer_type
ORDER BY total_paid DESC;


-- 5. Days in AR (Accounts Receivable) — claims aging
SELECT
    CASE
        WHEN days_to_adjudicate IS NULL                  THEN 'Not yet adjudicated'
        WHEN days_to_adjudicate <= 15                    THEN '0-15 days'
        WHEN days_to_adjudicate <= 30                    THEN '16-30 days'
        WHEN days_to_adjudicate <= 60                    THEN '31-60 days'
        WHEN days_to_adjudicate <= 90                    THEN '61-90 days'
        ELSE                                                  '90+ days'
    END AS ar_bucket,
    COUNT(*)                       AS claim_count,
    ROUND(SUM(billed_amount), 2)   AS billed_amount
FROM hive.curated.fact_claims
GROUP BY 1
ORDER BY MIN(COALESCE(days_to_adjudicate, 999));
