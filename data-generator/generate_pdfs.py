#!/usr/bin/env python3
"""
Generate synthetic medical PDFs — lab reports, discharge summaries, and prescriptions.

These mimic real-world clinical documents and feed the OCR / NLP pipeline in
spark/jobs/03_pdf_ocr_pipeline.py. All patient names, MRNs, and dates are random
(synthetic) — no real PHI is used.
"""
import argparse
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from tqdm import tqdm

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

OUT_DIR = Path(os.environ.get("PDF_OUT_DIR", "../data/raw/pdfs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

HOSPITAL_NAME    = "Pinewood Medical Center"
HOSPITAL_ADDR    = "1500 Health Plaza, Boston, MA 02118"
HOSPITAL_PHONE   = "(617) 555-0100"

styles = getSampleStyleSheet()
title_style = ParagraphStyle("title", parent=styles["Heading1"], alignment=1, spaceAfter=12)
section_style = ParagraphStyle("section", parent=styles["Heading2"], spaceAfter=6, textColor=colors.HexColor("#1f4e79"))
body_style = ParagraphStyle("body", parent=styles["BodyText"], spaceAfter=4)


def _patient():
    return {
        "name": fake.name(),
        "mrn": "".join(random.choices("0123456789", k=8)),
        "dob": fake.date_of_birth(minimum_age=20, maximum_age=85).strftime("%m/%d/%Y"),
        "sex": random.choice(["Male", "Female"]),
    }


def _provider():
    return f"Dr. {fake.last_name()}, {random.choice(['MD', 'DO'])}"


def make_lab_report(out_path):
    p = _patient()
    doc = SimpleDocTemplate(str(out_path), pagesize=LETTER,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.5*inch, bottomMargin=0.5*inch)
    story = []
    story.append(Paragraph(f"<b>{HOSPITAL_NAME}</b>", title_style))
    story.append(Paragraph(f"{HOSPITAL_ADDR} • Tel: {HOSPITAL_PHONE}", body_style))
    story.append(Spacer(1, 12))
    story.append(Paragraph("LABORATORY REPORT", title_style))

    info = [
        ["Patient Name:", p["name"], "MRN:", p["mrn"]],
        ["DOB:", p["dob"], "Sex:", p["sex"]],
        ["Collected:", fake.date_time_between(start_date="-30d").strftime("%m/%d/%Y %H:%M"),
         "Reported:", fake.date_time_between(start_date="-15d").strftime("%m/%d/%Y %H:%M")],
        ["Ordering Provider:", _provider(), "Specimen:", random.choice(["Serum", "Whole Blood", "Plasma", "Urine"])],
    ]
    t = Table(info, colWidths=[1.2*inch, 2.5*inch, 1.0*inch, 1.8*inch])
    t.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("BACKGROUND", (0,0), (0,-1), colors.HexColor("#f0f0f0")),
        ("BACKGROUND", (2,0), (2,-1), colors.HexColor("#f0f0f0")),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))

    panel = random.choice(["Comprehensive Metabolic Panel", "Complete Blood Count", "Lipid Panel", "Hemoglobin A1c", "Thyroid Function"])
    story.append(Paragraph(f"Test: {panel}", section_style))

    if panel == "Comprehensive Metabolic Panel":
        results = [
            ["Test", "Result", "Units", "Reference Range", "Flag"],
            ["Glucose",         f"{random.randint(70, 220)}",   "mg/dL", "70-99",       random.choice(["", "H", ""])],
            ["BUN",             f"{random.randint(7, 35)}",     "mg/dL", "7-20",        random.choice(["", "H", ""])],
            ["Creatinine",      f"{random.uniform(0.5, 2.4):.2f}", "mg/dL", "0.6-1.3", random.choice(["", "H", ""])],
            ["Sodium",          f"{random.randint(132, 146)}",  "mmol/L","135-145",     ""],
            ["Potassium",       f"{random.uniform(3.2, 5.5):.1f}", "mmol/L", "3.5-5.0", random.choice(["", "L", "H", ""])],
            ["Chloride",        f"{random.randint(96, 110)}",   "mmol/L","98-107",      ""],
            ["CO2",             f"{random.randint(20, 32)}",    "mmol/L","22-29",       ""],
            ["Calcium",         f"{random.uniform(8.0, 10.6):.1f}", "mg/dL", "8.6-10.3", ""],
            ["Total Protein",   f"{random.uniform(6.0, 8.5):.1f}", "g/dL", "6.0-8.3",   ""],
            ["Albumin",         f"{random.uniform(3.0, 5.2):.1f}", "g/dL", "3.5-5.0",   ""],
            ["Bilirubin",       f"{random.uniform(0.2, 1.5):.1f}", "mg/dL","0.1-1.2",   ""],
            ["AST",             f"{random.randint(10, 80)}",    "U/L",   "10-40",       random.choice(["", "H"])],
            ["ALT",             f"{random.randint(7, 90)}",     "U/L",   "7-56",        random.choice(["", "H"])],
        ]
    elif panel == "Complete Blood Count":
        results = [
            ["Test", "Result", "Units", "Reference Range", "Flag"],
            ["WBC",        f"{random.uniform(3.0, 15.0):.1f}",  "K/uL",   "4.5-11.0", random.choice(["", "H", "L"])],
            ["RBC",        f"{random.uniform(3.8, 5.8):.2f}",   "M/uL",   "4.2-5.4",  ""],
            ["Hemoglobin", f"{random.uniform(10.0, 17.5):.1f}", "g/dL",   "13.5-17.5",random.choice(["", "L"])],
            ["Hematocrit", f"{random.uniform(32.0, 52.0):.1f}", "%",      "38.8-50.0",""],
            ["Platelets",  f"{random.randint(120, 450)}",       "K/uL",   "150-400",  random.choice(["", "L", "H"])],
            ["MCV",        f"{random.uniform(78, 100):.1f}",    "fL",     "80-100",   ""],
            ["MCH",        f"{random.uniform(26, 34):.1f}",     "pg",     "27-31",    ""],
        ]
    elif panel == "Lipid Panel":
        results = [
            ["Test", "Result", "Units", "Reference Range", "Flag"],
            ["Total Cholesterol",  f"{random.randint(140, 280)}", "mg/dL", "<200",   random.choice(["", "H"])],
            ["HDL Cholesterol",    f"{random.randint(25, 75)}",   "mg/dL", ">40",    random.choice(["", "L"])],
            ["LDL Cholesterol",    f"{random.randint(70, 200)}",  "mg/dL", "<100",   random.choice(["", "H"])],
            ["Triglycerides",      f"{random.randint(50, 350)}",  "mg/dL", "<150",   random.choice(["", "H"])],
        ]
    elif panel == "Hemoglobin A1c":
        results = [
            ["Test", "Result", "Units", "Reference Range", "Flag"],
            ["Hemoglobin A1c", f"{random.uniform(4.5, 12.0):.1f}", "%", "<5.7", random.choice(["", "H"])],
            ["eAG",            f"{random.randint(80, 280)}",       "mg/dL", "<117", random.choice(["", "H"])],
        ]
    else:
        results = [
            ["Test", "Result", "Units", "Reference Range", "Flag"],
            ["TSH",        f"{random.uniform(0.3, 8.0):.2f}",  "uIU/mL", "0.4-4.5", random.choice(["", "H", "L"])],
            ["Free T4",    f"{random.uniform(0.6, 2.0):.2f}",  "ng/dL",  "0.8-1.8", ""],
            ["Free T3",    f"{random.uniform(2.0, 5.0):.2f}",  "pg/mL",  "2.3-4.2", ""],
        ]

    rt = Table(results, colWidths=[1.8*inch, 1.0*inch, 1.0*inch, 1.5*inch, 0.5*inch])
    rt.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#1f4e79")),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 9),
        ("ALIGN",      (1,1), (-1,-1), "CENTER"),
        ("GRID",       (0,0), (-1,-1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f8f8f8")]),
    ]))
    story.append(rt)
    story.append(Spacer(1, 12))

    story.append(Paragraph("Comments:", section_style))
    story.append(Paragraph(random.choice([
        "Results reviewed. No critical values requiring immediate notification.",
        "Recommend follow-up with primary care physician within 2 weeks.",
        "Elevated values noted. Clinical correlation advised.",
        "All values within expected reference ranges given patient history.",
    ]), body_style))

    doc.build(story)


def make_discharge_summary(out_path):
    p = _patient()
    doc = SimpleDocTemplate(str(out_path), pagesize=LETTER,
                            rightMargin=0.75*inch, leftMargin=0.75*inch)
    story = []
    story.append(Paragraph(f"<b>{HOSPITAL_NAME}</b>", title_style))
    story.append(Paragraph("DISCHARGE SUMMARY", title_style))

    admit_date = fake.date_between(start_date="-60d", end_date="-7d")
    discharge_date = admit_date + timedelta(days=random.randint(2, 14))

    info = [
        ["Patient:", p["name"], "MRN:", p["mrn"]],
        ["DOB:", p["dob"], "Sex:", p["sex"]],
        ["Admit Date:", admit_date.strftime("%m/%d/%Y"),
         "Discharge Date:", discharge_date.strftime("%m/%d/%Y")],
        ["Attending:", _provider(), "Service:", random.choice(["Medicine", "Surgery", "Cardiology", "ICU"])],
    ]
    t = Table(info, colWidths=[1.2*inch, 2.5*inch, 1.2*inch, 1.6*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("BACKGROUND", (0,0), (0,-1), colors.HexColor("#f0f0f0")),
        ("BACKGROUND", (2,0), (2,-1), colors.HexColor("#f0f0f0")),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))

    dx_pool = [
        ("I50.9",  "Heart failure, unspecified"),
        ("J44.1",  "COPD with acute exacerbation"),
        ("J18.9",  "Pneumonia, unspecified organism"),
        ("N17.9",  "Acute kidney failure, unspecified"),
        ("A41.9",  "Sepsis, unspecified organism"),
        ("I63.9",  "Cerebral infarction, unspecified"),
        ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
    ]
    primary_dx = random.choice(dx_pool)
    secondary_dx = random.sample([d for d in dx_pool if d != primary_dx], k=random.randint(1, 3))

    story.append(Paragraph("PRINCIPAL DIAGNOSIS", section_style))
    story.append(Paragraph(f"{primary_dx[0]} — {primary_dx[1]}", body_style))
    story.append(Spacer(1, 6))

    story.append(Paragraph("SECONDARY DIAGNOSES", section_style))
    for d in secondary_dx:
        story.append(Paragraph(f"• {d[0]} — {d[1]}", body_style))
    story.append(Spacer(1, 6))

    story.append(Paragraph("HOSPITAL COURSE", section_style))
    story.append(Paragraph(
        f"Patient is a {random.randint(40, 85)}-year-old {p['sex'].lower()} who presented to the "
        f"emergency department with {random.choice(['shortness of breath', 'chest pain', 'altered mental status', 'fever and chills', 'abdominal pain'])}. "
        f"On admission, vital signs were notable for {random.choice(['tachycardia', 'hypotension', 'fever to 102°F', 'oxygen saturation of 88% on room air'])}. "
        f"Patient was admitted to the {random.choice(['medical floor', 'telemetry unit', 'ICU'])} for further management. "
        f"Treatment included IV antibiotics, supportive care, and {random.choice(['diuresis', 'bronchodilators', 'insulin drip', 'pressors'])}. "
        f"Patient's condition improved over the course of admission and was deemed stable for discharge.", body_style))
    story.append(Spacer(1, 6))

    story.append(Paragraph("DISCHARGE MEDICATIONS", section_style))
    meds = random.sample([
        "Metformin 500mg BID", "Lisinopril 10mg daily", "Atorvastatin 40mg HS",
        "Aspirin 81mg daily", "Metoprolol succinate 50mg daily", "Furosemide 40mg daily",
        "Albuterol inhaler 2 puffs Q4H PRN", "Pantoprazole 40mg daily", "Levothyroxine 75mcg daily",
    ], k=random.randint(3, 6))
    for m in meds:
        story.append(Paragraph(f"• {m}", body_style))
    story.append(Spacer(1, 6))

    story.append(Paragraph("DISCHARGE INSTRUCTIONS", section_style))
    story.append(Paragraph(
        f"Patient discharged to {random.choice(['home', 'home with home health services', 'skilled nursing facility'])}. "
        f"Follow up with primary care physician in {random.randint(7, 14)} days. "
        f"Patient instructed on medication compliance, dietary restrictions, and to return to the ED for "
        f"worsening symptoms.", body_style))

    doc.build(story)


def make_prescription(out_path):
    p = _patient()
    doc = SimpleDocTemplate(str(out_path), pagesize=LETTER)
    story = []
    story.append(Paragraph(f"<b>{HOSPITAL_NAME}</b>", title_style))
    story.append(Paragraph(HOSPITAL_ADDR, body_style))
    story.append(Spacer(1, 12))
    story.append(Paragraph("PRESCRIPTION", title_style))

    info = [
        ["Patient:", p["name"]],
        ["DOB:", p["dob"]],
        ["MRN:", p["mrn"]],
        ["Date:", fake.date_this_month().strftime("%m/%d/%Y")],
        ["Prescriber:", _provider()],
    ]
    t = Table(info, colWidths=[1.5*inch, 4*inch])
    t.setStyle(TableStyle([("FONTSIZE", (0,0), (-1,-1), 10)]))
    story.append(t)
    story.append(Spacer(1, 24))

    rx = random.sample([
        ("Amoxicillin 500mg", "1 capsule by mouth three times daily for 10 days", "30"),
        ("Atorvastatin 40mg", "1 tablet by mouth at bedtime", "90"),
        ("Lisinopril 10mg",   "1 tablet by mouth once daily", "90"),
        ("Metformin 500mg",   "1 tablet by mouth twice daily with meals", "180"),
        ("Albuterol HFA",     "2 puffs inhaled every 4-6 hours as needed for shortness of breath", "1"),
        ("Pantoprazole 40mg", "1 tablet by mouth daily before breakfast", "90"),
    ], k=random.randint(1, 3))

    for med, sig, qty in rx:
        story.append(Paragraph(f"<b>Rx:</b> {med}", section_style))
        story.append(Paragraph(f"<b>Sig:</b> {sig}", body_style))
        story.append(Paragraph(f"<b>Disp:</b> {qty} &nbsp;&nbsp; <b>Refills:</b> {random.choice([0, 1, 2, 3, 5])}", body_style))
        story.append(Spacer(1, 12))

    story.append(Spacer(1, 24))
    story.append(Paragraph("_____________________________", body_style))
    story.append(Paragraph("Provider Signature", body_style))

    doc.build(story)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=100, help="Total PDFs to generate")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Generating {args.n} medical PDFs → {OUT_DIR.resolve()}")
    print("=" * 60)

    n_each = args.n // 3
    for i in tqdm(range(n_each), desc="lab reports"):
        make_lab_report(OUT_DIR / f"lab_report_{i:04d}.pdf")
    for i in tqdm(range(n_each), desc="discharge summaries"):
        make_discharge_summary(OUT_DIR / f"discharge_summary_{i:04d}.pdf")
    for i in tqdm(range(args.n - 2*n_each), desc="prescriptions"):
        make_prescription(OUT_DIR / f"prescription_{i:04d}.pdf")

    print(f"\n✓ Generated {args.n} PDFs in {OUT_DIR.resolve()}")
