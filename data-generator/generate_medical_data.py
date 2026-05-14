#!/usr/bin/env python3
"""
Generate synthetic medical & billing data and upload to MinIO raw zone.

Produces:
  - patients.csv               (10,000 rows)
  - providers.csv              (500 rows)
  - encounters.csv             (50,000 rows)
  - diagnoses.csv              (~120,000 rows, ~2.4 per encounter)
  - procedures.csv             (~90,000 rows, ~1.8 per encounter)
  - claims.csv                 (~50,000 rows, 1 per encounter)
  - payments.csv               (~45,000 rows, 0.9 per claim)
  - payers.csv                 (10 rows)
  - icd10_reference.csv        (reference)
  - cpt_reference.csv          (reference)
  - denial_reasons.csv         (reference)

Then uploads everything to MinIO under s3://raw/tables/
"""

import argparse
import os
import random
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent))
from medical_codes import (
    ICD10_DIAGNOSES, CPT_PROCEDURES, DENIAL_REASONS, PAYERS,
    ENCOUNTER_TYPES, DEPARTMENTS, CLAIM_STATUSES,
)

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

OUT_DIR = Path(os.environ.get("OUT_DIR", "../data/raw/tables"))
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _weighted_choice(items):
    """items = [(value, weight), ...]"""
    values, weights = zip(*items)
    return random.choices(values, weights=weights, k=1)[0]


def _mrn():
    """Medical Record Number — 8 digits"""
    return "".join(random.choices(string.digits, k=8))


def _claim_id():
    return "CLM-" + "".join(random.choices(string.digits, k=10))


# ──────────────────────────────────────────────────────────────────────
# 1. PATIENTS
# ──────────────────────────────────────────────────────────────────────
def gen_patients(n=10_000):
    print(f"Generating {n:,} patients…")
    rows = []
    for i in tqdm(range(n), desc="patients"):
        sex = random.choice(["M", "F"])
        first_name = fake.first_name_male() if sex == "M" else fake.first_name_female()
        dob = fake.date_of_birth(minimum_age=0, maximum_age=95)
        age = (datetime.now().date() - dob).days // 365
        rows.append({
            "patient_id":         f"P{1000000 + i}",
            "mrn":                _mrn(),
            "first_name":         first_name,
            "last_name":          fake.last_name(),
            "date_of_birth":      dob.isoformat(),
            "age":                age,
            "sex":                sex,
            "race":               random.choices(
                                      ["White","Black or African American","Asian","Hispanic or Latino",
                                       "American Indian or Alaska Native","Native Hawaiian or Pacific Islander",
                                       "Other","Unknown"],
                                      weights=[0.60,0.13,0.06,0.18,0.01,0.01,0.005,0.005], k=1)[0],
            "ethnicity":          random.choice(["Hispanic or Latino","Not Hispanic or Latino","Unknown"]),
            "marital_status":     random.choice(["Single","Married","Divorced","Widowed","Separated","Unknown"]),
            "language":           random.choices(["English","Spanish","Other"], weights=[0.78,0.18,0.04], k=1)[0],
            "ssn":                fake.ssn(),                     # PII — will be masked downstream
            "phone":              fake.phone_number(),            # PII
            "email":              fake.email(),                   # PII
            "address_line1":      fake.street_address(),          # PII
            "city":               fake.city(),
            "state":              fake.state_abbr(),
            "zip":                fake.zipcode(),
            "primary_payer_id":   _weighted_choice([(p[0], 1) for p in PAYERS]),
            "secondary_payer_id": random.choice([None]*7 + [p[0] for p in PAYERS]),
            "deceased":           random.choices([False, True], weights=[0.985, 0.015], k=1)[0],
            "created_at":         fake.date_time_between(start_date="-3y").isoformat(),
        })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "patients.csv", index=False)
    print(f"  ✓ patients.csv  ({len(df):,} rows)")
    return df


# ──────────────────────────────────────────────────────────────────────
# 2. PROVIDERS
# ──────────────────────────────────────────────────────────────────────
def gen_providers(n=500):
    print(f"Generating {n:,} providers…")
    rows = []
    for i in tqdm(range(n), desc="providers"):
        rows.append({
            "provider_id":   f"PR{10000 + i}",
            "npi":           "".join(random.choices(string.digits, k=10)),  # National Provider Identifier
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "credentials":   random.choice(["MD", "DO", "NP", "PA", "RN", "MD, FACC", "MD, FACP"]),
            "department":    random.choice(DEPARTMENTS),
            "specialty":     random.choice([
                "Internal Medicine","Cardiology","Family Medicine","Pediatrics",
                "Emergency Medicine","Hospitalist","Surgery","Radiology","Oncology"
            ]),
            "facility":      random.choice(["Main Hospital","North Campus","South Clinic","East Medical Center","Outpatient Plaza"]),
            "active":        random.choices([True, False], weights=[0.95, 0.05], k=1)[0],
            "hire_date":     fake.date_between(start_date="-15y").isoformat(),
        })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "providers.csv", index=False)
    print(f"  ✓ providers.csv ({len(df):,} rows)")
    return df


# ──────────────────────────────────────────────────────────────────────
# 3. ENCOUNTERS
# ──────────────────────────────────────────────────────────────────────
def gen_encounters(patients_df, providers_df, n=50_000):
    print(f"Generating {n:,} encounters…")
    patient_ids  = patients_df["patient_id"].tolist()
    provider_ids = providers_df[providers_df["active"]]["provider_id"].tolist()
    departments  = providers_df.set_index("provider_id")["department"].to_dict()

    rows = []
    for i in tqdm(range(n), desc="encounters"):
        pid = random.choice(patient_ids)
        prv = random.choice(provider_ids)
        etype = _weighted_choice(ENCOUNTER_TYPES)
        start = fake.date_time_between(start_date="-2y", end_date="now")
        if etype == "inpatient":
            duration_hours = random.randint(48, 720)
        elif etype == "emergency":
            duration_hours = random.randint(2, 12)
        else:
            duration_hours = random.choice([0, 1])
        end = start + timedelta(hours=duration_hours)
        rows.append({
            "encounter_id":   f"E{20000000 + i}",
            "patient_id":     pid,
            "provider_id":    prv,
            "department":     departments.get(prv, "Internal Medicine"),
            "encounter_type": etype,
            "admit_datetime": start.isoformat(),
            "discharge_datetime": end.isoformat() if etype in ("inpatient", "emergency") else None,
            "length_of_stay_hours": duration_hours,
            "admission_source": random.choice(["Physician Referral","Emergency Room","Transfer","Walk-in","Scheduled"]),
            "discharge_disposition": random.choice([
                "Home","Skilled Nursing","Home Health","Expired","Against Medical Advice",
                "Transferred","Home","Home","Home","Home"   # weight toward Home
            ]),
            "facility":       random.choice(["Main Hospital","North Campus","South Clinic","East Medical Center"]),
        })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "encounters.csv", index=False)
    print(f"  ✓ encounters.csv ({len(df):,} rows)")
    return df


# ──────────────────────────────────────────────────────────────────────
# 4. DIAGNOSES (one-to-many with encounters)
# ──────────────────────────────────────────────────────────────────────
def gen_diagnoses(encounters_df):
    print("Generating diagnoses (~2.4 per encounter)…")
    rows = []
    for enc_id in tqdm(encounters_df["encounter_id"], desc="diagnoses"):
        n_dx = random.choices([1, 2, 3, 4, 5], weights=[0.20, 0.30, 0.25, 0.15, 0.10], k=1)[0]
        chosen = random.sample(ICD10_DIAGNOSES, k=min(n_dx, len(ICD10_DIAGNOSES)))
        for rank, (code, desc, hcc, severity, raf) in enumerate(chosen, start=1):
            rows.append({
                "diagnosis_id":   f"DX{len(rows):010d}",
                "encounter_id":   enc_id,
                "icd10_code":     code,
                "description":    desc,
                "rank":           rank,
                "is_primary":     rank == 1,
                "hcc_category":   hcc if hcc > 0 else None,
                "severity":       severity,
                "raf_weight":     raf,
                "present_on_admission": random.choices(["Y", "N", "U", "W"], weights=[0.75, 0.15, 0.05, 0.05], k=1)[0],
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "diagnoses.csv", index=False)
    print(f"  ✓ diagnoses.csv ({len(df):,} rows)")
    return df


# ──────────────────────────────────────────────────────────────────────
# 5. PROCEDURES (one-to-many with encounters)
# ──────────────────────────────────────────────────────────────────────
def gen_procedures(encounters_df):
    print("Generating procedures (~1.8 per encounter)…")
    rows = []
    for enc_id in tqdm(encounters_df["encounter_id"], desc="procedures"):
        n_proc = random.choices([0, 1, 2, 3, 4], weights=[0.10, 0.40, 0.30, 0.15, 0.05], k=1)[0]
        if n_proc == 0:
            continue
        chosen = random.sample(CPT_PROCEDURES, k=min(n_proc, len(CPT_PROCEDURES)))
        for code, desc, category, base_charge in chosen:
            units = random.choices([1, 1, 1, 2, 3], weights=[0.7, 0.1, 0.05, 0.1, 0.05], k=1)[0]
            charge = round(base_charge * units * random.uniform(0.85, 1.25), 2)
            rows.append({
                "procedure_id":   f"PROC{len(rows):010d}",
                "encounter_id":   enc_id,
                "cpt_code":       code,
                "description":    desc,
                "category":       category,
                "units":          units,
                "charge_amount":  charge,
                "modifier":       random.choices([None, "25", "59", "26", "TC"], weights=[0.80, 0.05, 0.05, 0.05, 0.05], k=1)[0],
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "procedures.csv", index=False)
    print(f"  ✓ procedures.csv ({len(df):,} rows)")
    return df


# ──────────────────────────────────────────────────────────────────────
# 6. CLAIMS (one per encounter) and PAYMENTS
# ──────────────────────────────────────────────────────────────────────
def gen_claims_and_payments(encounters_df, procedures_df, patients_df):
    print("Generating claims and payments…")
    # Build charge per encounter
    charge_by_enc = procedures_df.groupby("encounter_id")["charge_amount"].sum().to_dict()
    patient_payer = patients_df.set_index("patient_id")["primary_payer_id"].to_dict()
    payer_collection = {p[0]: p[3] for p in PAYERS}
    denial_codes = [d[0] for d in DENIAL_REASONS]
    denial_weights = [d[2] for d in DENIAL_REASONS]

    claims = []
    payments = []
    for enc in tqdm(encounters_df.itertuples(), total=len(encounters_df), desc="claims"):
        enc_id = enc.encounter_id
        pid = enc.patient_id
        billed = charge_by_enc.get(enc_id, 0.0)
        if billed <= 0:
            continue
        payer_id = patient_payer.get(pid, "SELFPAY")
        submitted_dt = datetime.fromisoformat(enc.admit_datetime) + timedelta(days=random.randint(1, 14))
        status = _weighted_choice(CLAIM_STATUSES)
        cid = _claim_id()

        denial_reason = None
        allowed = 0.0
        paid_amt = 0.0
        patient_resp = 0.0
        adjustment = 0.0

        if status == "denied":
            denial_reason = random.choices(denial_codes, weights=denial_weights, k=1)[0]
            paid_amt = 0.0
            adjustment = billed * random.uniform(0.10, 0.30)
            patient_resp = billed - adjustment
        elif status == "paid":
            collection = payer_collection.get(payer_id, 0.7)
            allowed = billed * random.uniform(0.55, 0.95)
            paid_amt = allowed * collection
            patient_resp = allowed - paid_amt
            adjustment = billed - allowed
        elif status == "approved":
            allowed = billed * random.uniform(0.55, 0.95)
            adjustment = billed - allowed

        claims.append({
            "claim_id":        cid,
            "encounter_id":    enc_id,
            "patient_id":      pid,
            "payer_id":        payer_id,
            "billed_amount":   round(billed, 2),
            "allowed_amount":  round(allowed, 2),
            "paid_amount":     round(paid_amt, 2),
            "patient_responsibility": round(patient_resp, 2),
            "adjustment_amount":   round(adjustment, 2),
            "claim_status":    status,
            "denial_reason_code": denial_reason,
            "submitted_date":  submitted_dt.date().isoformat(),
            "adjudicated_date": (submitted_dt + timedelta(days=random.randint(7, 45))).date().isoformat() if status in ("paid","denied","approved") else None,
            "claim_type":      "professional" if random.random() > 0.3 else "institutional",
        })

        # Generate 0–2 payment records per paid claim
        if status == "paid":
            n_pay = random.choices([1, 2], weights=[0.85, 0.15], k=1)[0]
            remaining = paid_amt
            for j in range(n_pay):
                amt = remaining if j == n_pay - 1 else remaining * random.uniform(0.4, 0.6)
                payments.append({
                    "payment_id":    f"PMT{len(payments):010d}",
                    "claim_id":      cid,
                    "payer_id":      payer_id,
                    "payment_date":  (submitted_dt + timedelta(days=random.randint(14, 60))).date().isoformat(),
                    "payment_amount": round(amt, 2),
                    "payment_method": random.choice(["EFT", "Check", "Credit Card", "ACH"]),
                })
                remaining -= amt

    pd.DataFrame(claims).to_csv(OUT_DIR / "claims.csv", index=False)
    pd.DataFrame(payments).to_csv(OUT_DIR / "payments.csv", index=False)
    print(f"  ✓ claims.csv   ({len(claims):,} rows)")
    print(f"  ✓ payments.csv ({len(payments):,} rows)")


# ──────────────────────────────────────────────────────────────────────
# 7. REFERENCE TABLES
# ──────────────────────────────────────────────────────────────────────
def gen_reference():
    print("Generating reference tables…")
    pd.DataFrame(ICD10_DIAGNOSES, columns=["icd10_code","description","hcc_category","severity","raf_weight"])\
        .to_csv(OUT_DIR / "icd10_reference.csv", index=False)
    pd.DataFrame(CPT_PROCEDURES, columns=["cpt_code","description","category","base_charge"])\
        .to_csv(OUT_DIR / "cpt_reference.csv", index=False)
    pd.DataFrame(DENIAL_REASONS, columns=["denial_code","denial_description","frequency_weight"])\
        .to_csv(OUT_DIR / "denial_reasons.csv", index=False)
    pd.DataFrame(PAYERS, columns=["payer_id","payer_name","payer_type","avg_collection_rate"])\
        .to_csv(OUT_DIR / "payers.csv", index=False)
    print("  ✓ reference tables (icd10, cpt, denial_reasons, payers)")


# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients",   type=int, default=10_000)
    parser.add_argument("--providers",  type=int, default=500)
    parser.add_argument("--encounters", type=int, default=50_000)
    args = parser.parse_args()

    print("=" * 60)
    print(f"Medical Data Generator — output: {OUT_DIR.resolve()}")
    print("=" * 60)

    patients   = gen_patients(args.patients)
    providers  = gen_providers(args.providers)
    encounters = gen_encounters(patients, providers, args.encounters)
    diagnoses  = gen_diagnoses(encounters)
    procedures = gen_procedures(encounters)
    gen_claims_and_payments(encounters, procedures, patients)
    gen_reference()

    print("=" * 60)
    print("Done. All CSVs written to:", OUT_DIR.resolve())
    print("=" * 60)
