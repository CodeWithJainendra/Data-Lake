-- =====================================================================
-- PROVIDER PRODUCTIVITY & QUALITY KPIs
-- Per-physician metrics — encounter volume, charges, case mix index,
-- and documentation quality.
-- =====================================================================

-- 1. Top 25 providers by encounter volume
SELECT
    p.provider_name,
    p.specialty,
    p.department,
    k.encounter_count,
    ROUND(k.total_charges, 2)        AS total_charges,
    ROUND(k.avg_dx_per_encounter, 2) AS avg_dx,
    ROUND(k.avg_proc_per_encounter, 2) AS avg_proc,
    ROUND(k.avg_raf_score, 3)        AS avg_raf,
    k.severe_dx_encounters
FROM hive.curated.agg_provider_kpi k
JOIN hive.curated.dim_provider p USING (provider_id)
ORDER BY k.encounter_count DESC
LIMIT 25;


-- 2. Provider denial rate (quality signal — high denial rate suggests
--    documentation or coding issues)
SELECT
    p.provider_name,
    p.specialty,
    COUNT(*)                                            AS total_claims,
    SUM(CASE WHEN c.is_denied THEN 1 ELSE 0 END)        AS denied,
    ROUND(SUM(CASE WHEN c.is_denied THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                        AS denial_rate_pct,
    ROUND(SUM(c.billed_amount), 2)                      AS billed,
    ROUND(SUM(c.paid_amount), 2)                        AS paid
FROM hive.curated.fact_claims c
JOIN hive.curated.dim_provider p USING (provider_id)
GROUP BY p.provider_name, p.specialty
HAVING COUNT(*) > 20         -- only providers with meaningful volume
ORDER BY denial_rate_pct DESC
LIMIT 25;


-- 3. Provider case-mix index — average severity of patients they treat
SELECT
    p.provider_name,
    p.specialty,
    p.department,
    k.encounter_count,
    ROUND(k.avg_raf_score, 3)            AS avg_raf,
    ROUND(k.severe_dx_encounters * 100.0 / k.encounter_count, 2) AS severe_pct,
    ROUND(k.avg_los_hours / 24.0, 2)     AS avg_los_days
FROM hive.curated.agg_provider_kpi k
JOIN hive.curated.dim_provider p USING (provider_id)
WHERE k.encounter_count >= 10
ORDER BY k.avg_raf_score DESC
LIMIT 25;
