"""
Reference data for medical codes — realistic ICD-10 diagnoses, CPT procedures,
denial reason codes, and payer information.

In production, these come from CMS / WHO official files. For this demo we ship a
curated subset that covers the most common diagnoses and procedures in US
healthcare, including ones that frequently appear in CDI (Clinical Documentation
Improvement) audits.
"""

# ICD-10-CM diagnosis codes — top conditions by claim volume
# Format: (code, description, hcc_category, severity, raf_weight)
ICD10_DIAGNOSES = [
    ("E11.9",   "Type 2 diabetes mellitus without complications",            19,  "moderate", 0.105),
    ("E11.65",  "Type 2 diabetes mellitus with hyperglycemia",                18,  "severe",   0.318),
    ("E11.40",  "Type 2 diabetes with diabetic neuropathy, unspecified",      18,  "severe",   0.318),
    ("E11.22",  "Type 2 diabetes with diabetic chronic kidney disease",       18,  "severe",   0.318),
    ("I10",     "Essential (primary) hypertension",                            0,  "mild",     0.000),
    ("I11.0",   "Hypertensive heart disease with heart failure",              85,  "severe",   0.323),
    ("I25.10",  "Atherosclerotic heart disease of native coronary artery",    88,  "moderate", 0.140),
    ("I50.9",   "Heart failure, unspecified",                                 85,  "severe",   0.323),
    ("I50.32",  "Chronic diastolic (congestive) heart failure",               85,  "severe",   0.323),
    ("I48.91",  "Unspecified atrial fibrillation",                            96,  "moderate", 0.268),
    ("J44.0",   "COPD with acute lower respiratory infection",               111,  "severe",   0.328),
    ("J44.1",   "COPD with acute exacerbation",                              111,  "severe",   0.328),
    ("J45.909", "Unspecified asthma, uncomplicated",                          0,   "mild",     0.000),
    ("J18.9",   "Pneumonia, unspecified organism",                          115,  "moderate", 0.182),
    ("N17.9",   "Acute kidney failure, unspecified",                        135,  "severe",   0.302),
    ("N18.3",   "Chronic kidney disease, stage 3 (moderate)",               138,  "moderate", 0.069),
    ("N18.5",   "Chronic kidney disease, stage 5",                          136,  "severe",   0.234),
    ("N18.6",   "End stage renal disease",                                  136,  "severe",   0.289),
    ("F32.9",   "Major depressive disorder, single episode, unspecified",    59,  "moderate", 0.395),
    ("F41.9",   "Anxiety disorder, unspecified",                              0,   "mild",     0.000),
    ("F03.90",  "Unspecified dementia without behavioral disturbance",       52,  "severe",   0.346),
    ("G30.9",   "Alzheimer's disease, unspecified",                          52,  "severe",   0.346),
    ("M17.11",  "Unilateral primary osteoarthritis, right knee",              0,   "mild",     0.000),
    ("M54.5",   "Low back pain",                                              0,   "mild",     0.000),
    ("K21.9",   "Gastro-esophageal reflux disease without esophagitis",       0,   "mild",     0.000),
    ("Z79.4",   "Long term (current) use of insulin",                         0,   "mild",     0.000),
    ("E78.5",   "Hyperlipidemia, unspecified",                                0,   "mild",     0.000),
    ("R07.9",   "Chest pain, unspecified",                                    0,   "mild",     0.000),
    ("R10.9",   "Unspecified abdominal pain",                                 0,   "mild",     0.000),
    ("R55",     "Syncope and collapse",                                       0,   "mild",     0.000),
    ("U07.1",   "COVID-19",                                                 152,  "severe",   1.011),
    ("C50.911", "Malignant neoplasm of unspecified site, right female breast", 11, "severe",   0.572),
    ("C18.9",   "Malignant neoplasm of colon, unspecified",                  10,  "severe",   0.572),
    ("C61",     "Malignant neoplasm of prostate",                            12,  "severe",   0.572),
    ("S72.001A","Fracture of unspecified part of neck of right femur",      170,  "severe",   0.224),
    ("A41.9",   "Sepsis, unspecified organism",                               2,  "severe",   0.435),
    ("R65.20",  "Severe sepsis without septic shock",                         2,  "severe",   0.435),
    ("R65.21",  "Severe sepsis with septic shock",                            2,  "severe",   1.130),
    ("I63.9",   "Cerebral infarction, unspecified",                          99,  "severe",   0.224),
    ("G93.40",  "Encephalopathy, unspecified",                               80,  "severe",   0.343),
]

# CPT-4 procedure codes — common procedures
CPT_PROCEDURES = [
    ("99213", "Office visit, established patient, level 3",                            "evaluation", 100.0),
    ("99214", "Office visit, established patient, level 4",                            "evaluation", 150.0),
    ("99215", "Office visit, established patient, level 5",                            "evaluation", 210.0),
    ("99223", "Initial hospital care, comprehensive",                                  "evaluation", 280.0),
    ("99232", "Subsequent hospital care, expanded problem focused",                    "evaluation", 110.0),
    ("99291", "Critical care, first 30-74 minutes",                                    "critical",   330.0),
    ("99281", "Emergency department visit, problem focused",                           "emergency",   75.0),
    ("99284", "Emergency department visit, high severity",                             "emergency",  300.0),
    ("99285", "Emergency department visit, comprehensive",                             "emergency",  450.0),
    ("36415", "Routine venipuncture",                                                  "lab",          5.0),
    ("80053", "Comprehensive metabolic panel",                                         "lab",         15.0),
    ("85025", "Complete blood count with differential",                                "lab",         12.0),
    ("80061", "Lipid panel",                                                           "lab",         18.0),
    ("83036", "Hemoglobin A1c",                                                        "lab",         13.0),
    ("71046", "Chest X-ray, 2 views",                                                  "radiology",   60.0),
    ("72148", "MRI lumbar spine, without contrast",                                    "radiology",  450.0),
    ("74176", "CT abdomen and pelvis without contrast",                                "radiology",  380.0),
    ("93000", "Electrocardiogram, complete",                                           "cardiology",  35.0),
    ("93306", "Echocardiography, complete with Doppler",                               "cardiology", 240.0),
    ("45378", "Diagnostic colonoscopy",                                                "procedure",  750.0),
    ("47562", "Laparoscopic cholecystectomy",                                          "surgery",   2500.0),
    ("27447", "Total knee arthroplasty",                                               "surgery",  15000.0),
    ("33533", "Coronary artery bypass, single graft",                                  "surgery",  35000.0),
    ("90471", "Immunization administration",                                           "immun",       25.0),
    ("96372", "Therapeutic injection, subcutaneous or intramuscular",                  "injection",   30.0),
    ("99396", "Periodic comprehensive preventive exam, 40-64 years",                   "preventive", 200.0),
]

# Denial reason codes — CARC (Claim Adjustment Reason Codes)
DENIAL_REASONS = [
    ("CO-16",  "Claim/service lacks information or has submission/billing error(s)",  0.18),
    ("CO-50",  "Non-covered services - not deemed medically necessary",               0.12),
    ("CO-29",  "Time limit for filing has expired",                                   0.08),
    ("CO-11",  "Diagnosis inconsistent with procedure",                               0.15),
    ("CO-18",  "Duplicate claim/service",                                             0.10),
    ("CO-97",  "Benefit included in another procedure already adjudicated",           0.09),
    ("CO-151", "Payment adjusted because the payer deems insufficient documentation", 0.13),
    ("CO-204", "Service not covered under patient's current benefit plan",            0.07),
    ("CO-45",  "Charges exceed your contracted fee arrangement",                      0.05),
    ("PR-1",   "Deductible amount",                                                   0.03),
]

# Insurance payers (US)
PAYERS = [
    ("BCBS",       "Blue Cross Blue Shield",   "commercial",   0.78),
    ("AETNA",      "Aetna",                    "commercial",   0.75),
    ("CIGNA",      "Cigna",                    "commercial",   0.74),
    ("UHC",        "UnitedHealthcare",         "commercial",   0.76),
    ("HUMANA",     "Humana",                   "commercial",   0.73),
    ("MEDICARE",   "Medicare",                 "government",   0.82),
    ("MEDICAID",   "Medicaid",                 "government",   0.65),
    ("TRICARE",    "TRICARE",                  "government",   0.80),
    ("KAISER",     "Kaiser Permanente",        "commercial",   0.79),
    ("SELFPAY",    "Self-Pay (uninsured)",     "self",         0.25),
]

# Encounter / visit types
ENCOUNTER_TYPES = [
    ("outpatient",  0.55),
    ("emergency",   0.18),
    ("inpatient",   0.12),
    ("telehealth",  0.10),
    ("preventive",  0.05),
]

# Departments / Service lines
DEPARTMENTS = [
    "Internal Medicine", "Cardiology", "Oncology", "Orthopedics", "Neurology",
    "Pulmonology", "Nephrology", "Gastroenterology", "Endocrinology",
    "Emergency Medicine", "Family Practice", "Psychiatry", "Radiology",
    "General Surgery", "Pediatrics",
]

# Claim status workflow
CLAIM_STATUSES = [
    ("submitted",   0.05),
    ("in_review",   0.10),
    ("approved",    0.55),
    ("paid",        0.20),
    ("denied",      0.10),
]
