-- =====================================================================
-- CLINICAL DOCUMENTATION IMPROVEMENT (CDI) ANALYTICS
-- The bread-and-butter of a CDI team: catch under-documented severe
-- conditions, optimize HCC capture, monitor RAF score performance.
-- =====================================================================

-- 1. RAF score performance by department
SELECT
    department,
    COUNT(DISTINCT enc.encounter_id)   AS encounters,
    ROUND(AVG(enc.total_raf_score), 3) AS avg_raf_per_encounter,
    SUM(enc.has_severe_dx)             AS severe_dx_encounters,
    ROUND(SUM(enc.has_severe_dx) * 100.0 / COUNT(*), 2)
                                       AS severe_dx_capture_rate_pct
FROM hive.curated.fact_encounters enc
WHERE department IS NOT NULL
GROUP BY department
ORDER BY avg_raf_per_encounter DESC;


-- 2. Top 20 diagnoses contributing to RAF score
SELECT
    dx.icd10_code,
    dx.description,
    COUNT(*)                       AS diagnosis_count,
    ROUND(SUM(dx.raf_weight), 2)   AS total_raf_contribution,
    ROUND(AVG(dx.raf_weight), 3)   AS avg_raf_weight
FROM hive.curated.fact_diagnoses dx
WHERE dx.raf_weight > 0
GROUP BY dx.icd10_code, dx.description
ORDER BY total_raf_contribution DESC
LIMIT 20;


-- 3. CDI opportunity: encounters with severe symptoms but no severe diagnosis coded
-- (Heuristic: encounters with LOS > 5 days but no HCC-coded diagnosis)
SELECT
    enc.encounter_id,
    enc.department,
    enc.length_of_stay_hours / 24.0    AS los_days,
    enc.diagnosis_count,
    enc.primary_diagnosis_code,
    enc.primary_diagnosis_desc,
    enc.total_charge_amount
FROM hive.curated.fact_encounters enc
WHERE enc.length_of_stay_hours > 120     -- > 5 days
  AND enc.has_severe_dx = 0              -- no severe dx coded
  AND enc.encounter_type = 'inpatient'
ORDER BY enc.length_of_stay_hours DESC
LIMIT 50;


-- 4. Document coverage: which encounters have clinical PDFs attached?
SELECT
    enc.encounter_type,
    COUNT(DISTINCT enc.encounter_id)                                  AS total_encounters,
    COUNT(DISTINCT doc.document_id)                                   AS encounters_with_docs,
    ROUND(COUNT(DISTINCT doc.document_id) * 100.0 /
          COUNT(DISTINCT enc.encounter_id), 2)                         AS doc_coverage_pct
FROM hive.curated.fact_encounters enc
LEFT JOIN hive.curated.dim_clinical_documents doc
       ON doc.mrn IS NOT NULL  -- in real world, would join on patient_id via mrn
GROUP BY enc.encounter_type;


-- 5. Most common medications extracted from PDFs (NLP signal)
SELECT
    medication,
    COUNT(*) AS mention_count
FROM hive.curated.dim_clinical_documents
CROSS JOIN UNNEST(medications) AS t(medication)
GROUP BY medication
ORDER BY mention_count DESC
LIMIT 20;
