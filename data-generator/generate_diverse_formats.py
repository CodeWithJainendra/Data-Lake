#!/usr/bin/env python3
"""
Demonstrate the universal ingestion layer by emitting the SAME source data
in 6 different formats:

  data/raw/json/patients_sample.json     (single-doc JSON)
  data/raw/jsonl/encounters_sample.jsonl (newline-delimited JSON)
  data/raw/parquet/claims_sample.parquet (already-Parquet upstream)
  data/raw/excel/billing_sample.xlsx     (billing department export)
  data/raw/sql/patients_dump.sql         (DB dump with INSERT statements)
  data/raw/hl7/sample_messages.hl7       (pipe-delimited HL7 v2)
  data/raw/fhir/bundle_sample.json       (FHIR R4 Bundle)

Run AFTER generate_medical_data.py (uses its CSV outputs as the source).
"""
import json
import random
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE = Path(os.environ.get("BASE_DIR", "/home/jovyan/work/data/raw"))
SRC  = BASE / "tables"

(BASE / "json").mkdir(parents=True, exist_ok=True)
(BASE / "jsonl").mkdir(parents=True, exist_ok=True)
(BASE / "parquet").mkdir(parents=True, exist_ok=True)
(BASE / "excel").mkdir(parents=True, exist_ok=True)
(BASE / "sql").mkdir(parents=True, exist_ok=True)
(BASE / "hl7").mkdir(parents=True, exist_ok=True)
(BASE / "fhir").mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────
# 1. JSON (single doc array) — take first 200 patients
# ─────────────────────────────────────────────────────────────────────
print("Generating JSON…")
patients = pd.read_csv(SRC / "patients.csv").head(200)
(BASE / "json" / "patients_sample.json").write_text(
    json.dumps(patients.to_dict(orient="records"), default=str, indent=2)
)
print(f"  ✓ {BASE/'json'/'patients_sample.json'}  ({len(patients)} rows)")


# ─────────────────────────────────────────────────────────────────────
# 2. JSONL (newline-delimited) — take 500 encounters
# ─────────────────────────────────────────────────────────────────────
print("Generating JSONL…")
encounters = pd.read_csv(SRC / "encounters.csv").head(500)
with (BASE / "jsonl" / "encounters_sample.jsonl").open("w") as f:
    for _, row in encounters.iterrows():
        f.write(json.dumps({k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()},
                           default=str) + "\n")
print(f"  ✓ {BASE/'jsonl'/'encounters_sample.jsonl'}  ({len(encounters)} rows)")


# ─────────────────────────────────────────────────────────────────────
# 3. Parquet — take 1000 claims (already-processed upstream feed)
# ─────────────────────────────────────────────────────────────────────
print("Generating Parquet…")
claims = pd.read_csv(SRC / "claims.csv").head(1000)
claims.to_parquet(BASE / "parquet" / "claims_sample.parquet", index=False)
print(f"  ✓ {BASE/'parquet'/'claims_sample.parquet'}  ({len(claims)} rows)")


# ─────────────────────────────────────────────────────────────────────
# 4. Excel (.xlsx) — billing dept export, multiple sheets
# ─────────────────────────────────────────────────────────────────────
print("Generating Excel…")
with pd.ExcelWriter(BASE / "excel" / "billing_sample.xlsx", engine="openpyxl") as writer:
    pd.read_csv(SRC / "claims.csv").head(300).to_excel(writer, sheet_name="claims", index=False)
print(f"  ✓ {BASE/'excel'/'billing_sample.xlsx'}  (300 claim rows)")


# ─────────────────────────────────────────────────────────────────────
# 5. SQL dump — Postgres INSERT statements
# ─────────────────────────────────────────────────────────────────────
print("Generating SQL dump…")
patients_sql = pd.read_csv(SRC / "patients.csv").head(100)
cols = list(patients_sql.columns)
with (BASE / "sql" / "patients_dump.sql").open("w") as f:
    f.write("-- Postgres dump generated for data lake ingestion demo\n")
    f.write(f"-- Source: patients table   Generated: {datetime.utcnow().isoformat()}\n\n")
    f.write(f"INSERT INTO patients ({', '.join(cols)}) VALUES\n")
    rows_sql = []
    for _, row in patients_sql.iterrows():
        vals = []
        for v in row:
            if pd.isna(v):
                vals.append("NULL")
            else:
                s = str(v).replace("'", "''")
                vals.append(f"'{s}'")
        rows_sql.append("(" + ", ".join(vals) + ")")
    f.write(",\n".join(rows_sql))
    f.write(";\n")
print(f"  ✓ {BASE/'sql'/'patients_dump.sql'}  (100 INSERT rows)")


# ─────────────────────────────────────────────────────────────────────
# 6. HL7 v2 — ADT (admit, discharge, transfer) messages
# ─────────────────────────────────────────────────────────────────────
print("Generating HL7 v2…")
patients_hl7 = pd.read_csv(SRC / "patients.csv").head(50)
with (BASE / "hl7" / "sample_messages.hl7").open("w") as f:
    for _, p in patients_hl7.iterrows():
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        f.write(f"MSH|^~\\&|HOSP|FAC|LAB|RECV|{ts}||ADT^A01|MSG{random.randint(1,99999)}|P|2.5\n")
        f.write(f"EVN|A01|{ts}\n")
        f.write(f"PID|1||{p['mrn']}^^^HOSP^MR||{p['last_name']}^{p['first_name']}||"
                f"{str(p['date_of_birth']).replace('-','')}|{p['sex']}|||"
                f"{p.get('address_line1','')}^^{p.get('city','')}^{p.get('state','')}^{p.get('zip','')}\n")
        f.write(f"PV1|1|I|WARDA^101^A|||||||MED|||||||A0\n")
        f.write("\n")
print(f"  ✓ {BASE/'hl7'/'sample_messages.hl7'}  (50 ADT messages)")


# ─────────────────────────────────────────────────────────────────────
# 7. FHIR R4 Bundle — Patient + Encounter resources
# ─────────────────────────────────────────────────────────────────────
print("Generating FHIR Bundle…")
patients_fhir = pd.read_csv(SRC / "patients.csv").head(20)
bundle = {
    "resourceType": "Bundle",
    "type": "collection",
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "entry": []
}
for _, p in patients_fhir.iterrows():
    bundle["entry"].append({
        "resource": {
            "resourceType": "Patient",
            "id": str(p["patient_id"]),
            "identifier": [{"system": "urn:oid:hospital-mrn", "value": str(p["mrn"])}],
            "name": [{"family": str(p["last_name"]), "given": [str(p["first_name"])]}],
            "gender": "male" if p["sex"] == "M" else "female",
            "birthDate": str(p["date_of_birth"]),
            "address": [{
                "line": [str(p.get("address_line1", ""))],
                "city": str(p.get("city", "")),
                "state": str(p.get("state", "")),
                "postalCode": str(p.get("zip", "")),
            }],
        }
    })
(BASE / "fhir" / "bundle_sample.json").write_text(json.dumps(bundle, indent=2, default=str))
print(f"  ✓ {BASE/'fhir'/'bundle_sample.json'}  ({len(bundle['entry'])} Patient resources)")


print("\n" + "=" * 60)
print("Done. 7 alternate-format files written under", BASE)
print("=" * 60)
