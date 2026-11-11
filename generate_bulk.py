"""
Bulk generator: ~200 randomized synthetic in-patient packets (5 pages each) for testing
Databricks AI Functions (ai_parse_document / ai_classify / ai_extract) at volume.

Reuses the rendering of generate_packets.py (same 5-page layout, images, planted cross-page
dependencies and defects) with randomized hospitals, patients, clinical cases, dates and amounts.
All people, hospitals, insurers, IDs and amounts are FICTIONAL.

Outputs
  pdfs/pkt_NNNN_<hospital>_<case>.pdf     200 packets (the 3 original packets are left untouched)
  pdfs/upload_dup_N.pdf                    4 byte-identical copies of random packets (duplicate-detection test)
  01b_ground_truth_bulk.sql                answer key for ALL packets (3 originals + 200), same gt_* tables
  ground_truth_bulk.json                   the same answer key as JSON
  bulk_manifest.csv                        one row per file: hospital, case, dates, defects, duplicate_of

Usage:  python generate_bulk.py [count]      (default 200; needs reportlab + Pillow)
"""
import csv, json, os, random, shutil, sys, tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta

import generate_packets as gp

SEED = 20261111
DEFAULT_COUNT = 200
N_DUPLICATES = 4
DEFECT_RATE = 0.30          # ~30% of packets carry exactly one defect

# ---------------------------------------------------------------- reference data (FICTIONAL)
HOSPITALS = [
    dict(slug="lotusvalley", name="Lotus Valley Multispeciality Hospital", initials="LV", theme=(194, 65, 12),
         address="Plot 14, Financial District Road, Hyderabad, Telangana 500032", state="36", phone="+91 40 6612 4400",
         uhid="LVH-{yyyy}-{n6}", ip="IP-{yy}-{n5}", bill="LVH/IP/{yyyy}/{n5}", council="TSMC", tier=1.15),
    dict(slug="greenfield", name="Greenfield Surgical & Diabetes Centre", initials="GF", theme=(21, 128, 61),
         address="22 Baner Road, Pune, Maharashtra 411045", state="27", phone="+91 20 4910 2200",
         uhid="GSC-{n3}-{n5}", ip="ADM/{yyyy}/{n4}", bill="GSC-FB-{yy}-{n5}", council="MMC", tier=1.05),
    dict(slug="riverbend", name="Riverbend General Hospital", initials="RG", theme=(30, 64, 175),
         address="7 Kamarajar Salai, Mylapore, Chennai, Tamil Nadu 600004", state="33", phone="+91 44 2811 9000",
         uhid="RGH{n7}", ip="RGH-IP-{n5}", bill="RGH/{yyyy}/FB/{n5}", council="TNMC", tier=0.90),
    dict(slug="tamarindgrove", name="Tamarind Grove Hospital", initials="TG", theme=(124, 58, 237),
         address="45 Old Airport Road, Bengaluru, Karnataka 560017", state="29", phone="+91 80 4120 7700",
         uhid="TGH/{n6}", ip="TG-IP-{yyyy}-{n4}", bill="TGH-INV-{n6}", council="KMC", tier=1.25),
    dict(slug="saffronlake", name="Saffron Lake Medical Institute", initials="SL", theme=(202, 138, 4),
         address="SG Highway, Bodakdev, Ahmedabad, Gujarat 380054", state="24", phone="+91 79 6190 3300",
         uhid="SLMI{yy}{n6}", ip="IPD/{n6}", bill="SLMI/FB/{yy}{n5}", council="GMC", tier=1.00),
    dict(slug="indusmeadows", name="Indus Meadows Hospital", initials="IM", theme=(190, 18, 60),
         address="Tonk Road, Durgapura, Jaipur, Rajasthan 302018", state="08", phone="+91 141 271 5500",
         uhid="IMH-{n5}-{yy}", ip="IMH/IPD/{yy}/{n5}", bill="IMH-BILL-{yyyy}-{n5}", council="RMC", tier=0.85),
    dict(slug="neelgiricrest", name="Neelgiri Crest Hospital", initials="NC", theme=(13, 148, 136),
         address="Avinashi Road, Peelamedu, Coimbatore, Tamil Nadu 641004", state="33", phone="+91 422 450 8800",
         uhid="NCH {n4} {n4}", ip="NC/IP/{n5}", bill="NCH/{fy}/{n5}", council="TNMC", tier=0.95),
    dict(slug="brahmaputraview", name="Brahmaputra View Hospital", initials="BV", theme=(71, 85, 105),
         address="GS Road, Christian Basti, Guwahati, Assam 781005", state="18", phone="+91 361 234 6600",
         uhid="BVH-{n6}", ip="BVH-ADM-{n5}", bill="BVH/FB/{yyyy}/{n4}", council="AMC", tier=0.90),
]

INSURERS = [
    dict(name="Bharat Shield Health Insurance Ltd", policy="BSH-HLT-{n6}", member="BSH-M-{n4}-{n4}", ref="BSH/CL/{yyyy}/{n6}"),
    dict(name="Sahyadri General Insurance Co.", policy="SGI-FAM-{n6}", member="SGI-{n5}-0{n1}", ref="SGI/AUTH/{yy}/{n6}"),
    dict(name="Coastal Assurance India Ltd", policy="CAI-SR-{n6}", member="CAI-{n6}-01", ref="CAI/CL/{yyyy}/{n5}"),
    dict(name="Meridian Health Assurance Ltd", policy="MHA/{n8}", member="MHA-MBR-{n7}", ref="MHA-PA-{yy}{n6}"),
    dict(name="Pinnacle Care General Insurance", policy="PCG-IND-{n6}-{yy}", member="PCG{n9}", ref="PCG/AUTH/{n7}"),
    dict(name="Shakti Mutual Health Insurance", policy="SMH{n10}", member="SMH-M{n6}", ref="SMH/CLM/{yyyy}/{n6}"),
]

MALE = ["Ravi", "Arjun", "Suresh", "Vikram", "Mohammed", "Rahul", "Karthik", "Anil", "Joseph", "Harpreet", "Sanjay",
        "Imran", "Pradeep", "Venkatesh", "Rohit", "Abhishek", "Gaurav", "Naveen", "Faisal", "Deepak", "Manoj", "Ajay"]
FEMALE = ["Ananya", "Priya", "Lakshmi", "Fatima", "Sneha", "Kavya", "Meera", "Pooja", "Shabana", "Divya", "Nandini",
          "Rekha", "Sunita", "Aishwarya", "Mary", "Gurpreet", "Swati", "Ritu", "Asha", "Zainab", "Bhavna", "Neha"]
LAST = ["Menon", "Deshpande", "Iyer", "Sharma", "Reddy", "Khan", "Patel", "Nair", "Gupta", "Das", "Bose", "Kulkarni",
        "Rao", "Pillai", "Singh", "Joshi", "Chatterjee", "Fernandes", "Mehta", "Verma", "Hegde", "Banerjee", "Siddiqui",
        "Thomas", "Kaur", "Bhat", "Mishra", "Shetty", "Ansari", "Gowda", "Sinha", "Naidu"]
DOC_FIRST = ["Sanjana", "Kedar", "Lakshmi", "Arvind", "Farah", "Rajesh", "Shalini", "Nikhil", "Vandana", "Thomas",
             "Rupa", "Sameer", "Geeta", "Harish", "Nusrat", "Prakash", "Leena", "Aditya"]

# lab: (key, name, unit, ref_low, ref_high, charge, sample_min, sample_max, decimals)
L = dict(
    hb=("hb", "Haemoglobin", "g/dL", 13.0, 17.0, 250),
    wbc=("wbc", "Total WBC count", "x10^3/uL", 4.0, 11.0, 250),
    plt=("plt", "Platelet count", "x10^3/uL", 150, 410, 200),
    hba1c=("hba1c", "HbA1c", "%", 4.0, 5.6, 650),
    fbs=("fbs", "Fasting blood glucose", "mg/dL", 70, 100, 150),
    rbs=("rbs", "Random blood glucose", "mg/dL", 70, 140, 150),
    ldl=("ldl", "LDL cholesterol", "mg/dL", 0, 100, 400),
    trop=("trop", "Troponin I", "ng/mL", 0, 0.04, 1200),
    cr=("cr", "Serum creatinine", "mg/dL", 0.7, 1.3, 300),
    crp=("crp", "C-reactive protein", "mg/L", 0, 5, 600),
    pct=("pct", "Procalcitonin", "ng/mL", 0, 0.5, 1800),
    na=("na", "Serum sodium", "mmol/L", 135, 145, 280),
    k=("k", "Serum potassium", "mmol/L", 3.5, 5.1, 250),
    bicarb=("bicarb", "Serum bicarbonate", "mmol/L", 22, 29, 300),
    bili=("bili", "Total bilirubin", "mg/dL", 0.3, 1.2, 300),
    alp=("alp", "Alkaline phosphatase", "U/L", 44, 147, 300),
    sgpt=("sgpt", "SGPT (ALT)", "U/L", 7, 56, 250),
    amy=("amy", "Serum amylase", "U/L", 28, 100, 450),
    hct=("hct", "Haematocrit", "%", 40, 50, 200),
    esr=("esr", "ESR", "mm/hr", 0, 20, 150),
    ua=("ua", "Serum uric acid", "mg/dL", 3.4, 7.0, 250),
    ca=("ca", "Serum calcium", "mg/dL", 8.5, 10.5, 250),
    upc=("upc", "Urine pus cells", "/hpf", 0, 5, 200),
)
def lab(key, lo, hi, dec, ref=None):
    base = L[key]
    if ref:  # override reference range (e.g. female haemoglobin)
        base = base[:3] + ref + base[5:]
    return base + (lo, hi, dec)

CASES = [
    dict(slug="cardiac", specialty="MD, DM (Cardiology)", opd="Cardiology", age=(45, 78), los=(3, 6), icu=(1, 2),
         diagnosis="Acute NSTEMI (non-ST elevation myocardial infarction) with Type 2 Diabetes Mellitus",
         procedure="PCI to LAD with 1 drug-eluting stent",
         complaint="Retrosternal chest pain radiating to the left arm for {hrs} hours, with sweating.",
         course=("ECG at admission showed ST depression in the lateral leads. Troponin I was elevated ({trop} ng/mL). "
                 "Coronary angiography revealed critical stenosis of the proximal LAD, treated with PCI and one "
                 "drug-eluting stent. The patient was shifted from ICU to the ward after {icu_days} day(s). "
                 "HbA1c was {hba1c}%, and diabetes treatment was intensified."),
         ecg="12-lead ECG (lead V5) at admission",
         vitals=[("Blood pressure", "148/92 mmHg", "126/80 mmHg"), ("Pulse", "104 /min", "78 /min"),
                 ("SpO2", "95 %", "98 %"), ("Temperature", "98.8 F", "98.4 F")],
         services=[("Cath Lab Procedure - PCI with 1 DES", (1, 1), (120000, 165000)), ("Consultant Visits", (4, 10), (800, 1500)),
                   ("2D Echocardiography", (1, 1), (1800, 3200)), ("Radiology - Chest X-Ray PA view", (1, 1), (500, 800))],
         labs=[lab("hb", 11.0, 14.8, 1), lab("wbc", 8.0, 13.5, 1), lab("plt", 160, 380, 0), lab("hba1c", 6.2, 10.5, 1),
               lab("fbs", 110, 220, 0), lab("ldl", 110, 190, 0), lab("trop", 0.8, 6.5, 2), lab("cr", 0.8, 1.5, 1)],
         trend=("hba1c", "history"),
         meds=[("Aspirin", "75 mg tab", 1, (3, 5), 2.50), ("Clopidogrel", "75 mg tab", 1, (3, 5), 9.80),
               ("Atorvastatin", "80 mg tab", 1, (3, 5), 18.00), ("Enoxaparin", "60 mg inj", 2, (2, 3), 485.00),
               ("Metformin", "500 mg tab", 2, (3, 5), 3.20), ("Insulin Glargine", "100 IU/mL pen", None, None, 1150.00),
               ("Cath lab consumable kit", "kit", None, None, 8600.00)],
         rx=["Tab Aspirin 75 mg - once daily after lunch - to continue", "Tab Clopidogrel 75 mg - once daily - 12 months",
             "Tab Atorvastatin 80 mg - once daily at night - to continue", "Tab Metformin 500 mg - twice daily after food - to continue"]),
    dict(slug="cholecystectomy", specialty="MS (General Surgery)", opd="Surgery", age=(28, 62), los=(2, 4), icu=(0, 0),
         diagnosis="Acute calculous cholecystitis", procedure="Laparoscopic cholecystectomy",
         complaint="Right upper abdominal pain for {days} days with fever and vomiting.",
         course=("Ultrasound abdomen showed a distended gallbladder with multiple calculi and wall thickening. "
                 "Total bilirubin was {bili} mg/dL at admission. Laparoscopic cholecystectomy was performed on day 1 under "
                 "general anaesthesia. Recovery was uneventful and an oral diet was tolerated from day 2."),
         ecg="Pre-operative ECG (lead II) - normal sinus rhythm",
         vitals=[("Blood pressure", "132/84 mmHg", "118/76 mmHg"), ("Pulse", "98 /min", "80 /min"),
                 ("SpO2", "98 %", "99 %"), ("Temperature", "100.6 F", "98.6 F")],
         services=[("Laparoscopic Cholecystectomy - surgeon package", (1, 1), (55000, 75000)),
                   ("Operation Theatre Charges", (1, 1), (14000, 20000)), ("Anaesthesia Charges", (1, 1), (9000, 13000)),
                   ("Consultant Visits", (3, 6), (800, 1200)), ("Ultrasound Abdomen", (1, 1), (1400, 2200))],
         labs=[lab("hb", 11.5, 14.5, 1), lab("wbc", 10.5, 15.5, 1), lab("bili", 1.4, 3.2, 1), lab("alp", 150, 260, 0),
               lab("sgpt", 50, 120, 0), lab("amy", 40, 95, 0), lab("fbs", 80, 105, 0)],
         trend=("bili", "admission"),
         meds=[("Ceftriaxone", "1 g inj", 2, (2, 3), 64.00), ("Pantoprazole", "40 mg inj", 1, (2, 3), 52.00),
               ("Paracetamol", "1 g IV infusion", 3, (1, 2), 145.00), ("Ondansetron", "4 mg inj", 2, (1, 2), 18.50),
               ("Laparoscopy consumable kit", "kit", None, None, 14500.00)],
         rx=["Tab Cefuroxime 500 mg - twice daily - 5 days", "Tab Pantoprazole 40 mg - once daily before breakfast - 14 days",
             "Tab Paracetamol 650 mg - when required for pain, max 3 per day - 5 days"]),
    dict(slug="pneumonia", specialty="MD (Pulmonology)", opd="Pulmonology", age=(50, 86), los=(4, 8), icu=(0, 3),
         diagnosis="Community-acquired pneumonia, right lower lobe", procedure="Non-invasive ventilation (BiPAP)",
         complaint="Fever, productive cough and breathlessness for {days} days.",
         course=("Chest X-ray showed right lower lobe consolidation. CRP was {crp} mg/L and procalcitonin {pct} ng/mL at "
                 "admission. The patient received IV piperacillin-tazobactam and supplemental oxygen, with BiPAP support "
                 "as required. Inflammatory markers improved steadily before discharge."),
         ecg="ECG (lead II) at admission - sinus tachycardia",
         vitals=[("Blood pressure", "104/66 mmHg", "122/78 mmHg"), ("Pulse", "116 /min", "84 /min"),
                 ("SpO2", "86 % (room air)", "96 % (room air)"), ("Temperature", "102.4 F", "98.6 F")],
         services=[("Non-invasive ventilation (BiPAP) per day", (1, 3), (2800, 4000)), ("Consultant Visits", (5, 12), (700, 1100)),
                   ("Radiology - Chest X-Ray PA view", (1, 3), (500, 700)), ("Physiotherapy sessions", (2, 6), (400, 600))],
         labs=[lab("hb", 10.5, 13.5, 1), lab("wbc", 12.5, 19.5, 1), lab("crp", 60, 180, 0), lab("pct", 0.8, 4.5, 1),
               lab("cr", 0.9, 1.8, 1), lab("na", 128, 138, 0)],
         trend=("crp", "admission"),
         meds=[("Piperacillin-Tazobactam", "4.5 g inj", 3, (3, 6), 420.00), ("Azithromycin", "500 mg tab", 1, (3, 5), 28.00),
               ("Budesonide nebulisation", "0.5 mg respule", 2, (3, 6), 22.00), ("Paracetamol", "650 mg tab", 3, (3, 5), 2.10),
               ("Oxygen therapy", "per hour", None, None, 2700.00)],
         rx=["Tab Amoxicillin-Clavulanate 625 mg - three times daily - 5 days", "Budesonide 0.5 mg nebulisation - twice daily - 7 days",
             "Tab Paracetamol 650 mg - when required for fever - 3 days"]),
    dict(slug="appendicitis", specialty="MS (General Surgery)", opd="Surgery", age=(16, 45), los=(2, 4), icu=(0, 0),
         diagnosis="Acute appendicitis", procedure="Laparoscopic appendicectomy",
         complaint="Periumbilical pain shifting to the right iliac fossa for {days} day(s), with nausea and low-grade fever.",
         course=("Ultrasound showed an inflamed, non-compressible appendix. WBC count was {wbc} x10^3/uL with neutrophilia "
                 "and CRP {crp} mg/L. Laparoscopic appendicectomy was performed on day 1; the appendix was inflamed without "
                 "perforation. The patient was discharged on an oral diet."),
         ecg="Pre-operative ECG (lead II) - normal sinus rhythm",
         vitals=[("Blood pressure", "124/78 mmHg", "116/74 mmHg"), ("Pulse", "102 /min", "78 /min"),
                 ("SpO2", "99 %", "99 %"), ("Temperature", "100.2 F", "98.4 F")],
         services=[("Laparoscopic Appendicectomy - surgeon package", (1, 1), (38000, 58000)),
                   ("Operation Theatre Charges", (1, 1), (11000, 17000)), ("Anaesthesia Charges", (1, 1), (7000, 11000)),
                   ("Consultant Visits", (3, 6), (800, 1200)), ("Ultrasound Abdomen", (1, 1), (1400, 2200))],
         labs=[lab("hb", 11.8, 15.5, 1), lab("wbc", 11.5, 17.5, 1), lab("crp", 25, 140, 0), lab("cr", 0.6, 1.2, 1),
               lab("na", 134, 142, 0), lab("fbs", 78, 108, 0)],
         trend=("crp", "admission"),
         meds=[("Ceftriaxone", "1 g inj", 2, (2, 3), 64.00), ("Metronidazole", "500 mg inj", 3, (2, 3), 38.00),
               ("Pantoprazole", "40 mg inj", 1, (2, 3), 52.00), ("Paracetamol", "1 g IV infusion", 3, (1, 2), 145.00),
               ("Laparoscopy consumable kit", "kit", None, None, 11800.00)],
         rx=["Tab Cefuroxime 500 mg - twice daily - 5 days", "Tab Metronidazole 400 mg - three times daily - 5 days",
             "Tab Paracetamol 650 mg - when required for pain - 5 days"]),
    dict(slug="dengue", specialty="MD (General Medicine)", opd="Medicine", age=(18, 60), los=(3, 6), icu=(0, 1),
         diagnosis="Dengue fever (NS1 positive) with thrombocytopenia",
         procedure="Conservative management with IV fluids and platelet monitoring",
         complaint="High-grade fever, body ache and headache for {days} days.",
         course=("NS1 antigen was positive. Platelet count was {plt} x10^3/uL at admission and was monitored daily; there were "
                 "no bleeding manifestations. Haematocrit was checked 12-hourly and IV fluids were titrated. Platelets recovered "
                 "and the patient was afebrile for 48 hours before discharge."),
         ecg="ECG (lead II) at admission - normal sinus rhythm",
         vitals=[("Blood pressure", "108/70 mmHg", "118/76 mmHg"), ("Pulse", "96 /min", "76 /min"),
                 ("SpO2", "98 %", "99 %"), ("Temperature", "103.0 F", "98.4 F")],
         services=[("Consultant Visits", (4, 8), (700, 1100)), ("IV fluids and nursing care (per day)", (3, 6), (600, 900)),
                   ("Blood bank screening and cross-match", (1, 1), (1200, 1800))],
         labs=[lab("hb", 12.0, 16.5, 1), lab("hct", 38, 52, 1), lab("plt", 18, 90, 0), lab("wbc", 2.1, 4.5, 1),
               lab("sgpt", 45, 180, 0)],
         trend=("plt", "admission"),
         meds=[("Paracetamol", "650 mg tab", 3, (3, 5), 2.10), ("Ringer lactate", "500 mL infusion", 3, (2, 4), 65.00),
               ("Pantoprazole", "40 mg inj", 1, (2, 4), 52.00), ("Ondansetron", "4 mg inj", 2, (1, 2), 18.50)],
         rx=["Tab Paracetamol 650 mg - when required for fever, max 4 per day - 5 days", "ORS - 2 litres per day - 5 days",
             "Avoid NSAIDs (ibuprofen, diclofenac) until platelet count is normal"]),
    dict(slug="kneereplacement", specialty="MS (Orthopaedics)", opd="Orthopaedics", age=(55, 78), los=(4, 7), icu=(0, 0),
         diagnosis="Primary osteoarthritis, right knee (Kellgren-Lawrence grade IV)",
         procedure="Total knee replacement, right (cemented)",
         complaint="Progressive right knee pain and difficulty walking for {yrs} years, not relieved by medication.",
         course=("Pre-operative evaluation was satisfactory (haemoglobin {hb} g/dL). Right total knee replacement was performed "
                 "under spinal anaesthesia on day 1. Physiotherapy began on day 2 and the patient walked with a walker by day 3. "
                 "The wound was healthy at discharge."),
         ecg="Pre-operative ECG (lead II) - normal sinus rhythm",
         vitals=[("Blood pressure", "138/86 mmHg", "128/80 mmHg"), ("Pulse", "82 /min", "78 /min"),
                 ("SpO2", "98 %", "98 %"), ("Temperature", "98.4 F", "98.6 F")],
         services=[("Implant - knee prosthesis (cemented)", (1, 1), (90000, 160000)), ("Surgeon Fee - TKR", (1, 1), (40000, 60000)),
                   ("Operation Theatre Charges", (1, 1), (25000, 35000)), ("Anaesthesia Charges", (1, 1), (12000, 18000)),
                   ("Physiotherapy sessions", (3, 6), (500, 800)), ("Radiology - X-Ray knee AP/lateral", (1, 2), (600, 900))],
         labs=[lab("hb", 11.5, 14.5, 1), lab("wbc", 5.0, 10.0, 1), lab("fbs", 85, 140, 0), lab("cr", 0.7, 1.3, 1),
               lab("esr", 12, 45, 0)],
         trend=("hb", "admission"),
         meds=[("Cefuroxime", "1.5 g inj", 2, (2, 3), 110.00), ("Enoxaparin", "40 mg inj", 1, (3, 5), 365.00),
               ("Paracetamol", "1 g IV infusion", 3, (2, 3), 145.00), ("Pantoprazole", "40 mg inj", 1, (3, 5), 52.00),
               ("Orthopaedic implant consumables", "kit", None, None, 16800.00)],
         rx=["Tab Rivaroxaban 10 mg - once daily - 14 days", "Tab Paracetamol 650 mg - three times daily - 7 days",
             "Tab Pantoprazole 40 mg - once daily before breakfast - 14 days"]),
    dict(slug="dka", specialty="MD, DM (Endocrinology)", opd="Endocrinology", age=(15, 35), los=(3, 6), icu=(1, 2),
         diagnosis="Diabetic ketoacidosis with Type 1 Diabetes Mellitus",
         procedure="IV insulin infusion and fluid resuscitation (DKA protocol)",
         complaint="Vomiting, abdominal pain and drowsiness for {days} day(s); missed insulin doses.",
         course=("Capillary glucose was {rbs} mg/dL with ketonuria and metabolic acidosis (bicarbonate {bicarb} mmol/L). The "
                 "patient was managed in ICU on the DKA protocol for {icu_days} day(s) with IV fluids, insulin infusion and "
                 "potassium replacement, then switched to a basal-bolus insulin regimen."),
         ecg="ECG (lead II) at admission - sinus tachycardia",
         vitals=[("Blood pressure", "98/62 mmHg", "116/74 mmHg"), ("Pulse", "122 /min", "82 /min"),
                 ("SpO2", "97 %", "99 %"), ("Temperature", "98.8 F", "98.4 F")],
         services=[("Consultant Visits", (4, 9), (800, 1400)), ("Arterial blood gas analysis", (3, 6), (900, 1300)),
                   ("Diabetes educator session", (1, 1), (800, 1200))],
         labs=[lab("rbs", 320, 560, 0), lab("bicarb", 8, 15, 0), lab("k", 3.0, 5.6, 1), lab("na", 128, 138, 0),
               lab("cr", 1.0, 1.8, 1), lab("hba1c", 9.0, 13.0, 1)],
         trend=("rbs", "admission"),
         meds=[("Insulin Regular", "100 IU/mL vial", None, None, 180.00), ("Normal saline", "1 L infusion", 4, (2, 2), 72.00),
               ("Potassium chloride", "15% inj", 2, (2, 2), 34.00), ("Ondansetron", "4 mg inj", 2, (1, 2), 18.50),
               ("Insulin Glargine", "100 IU/mL pen", None, None, 1150.00)],
         rx=["Inj Insulin Glargine 18 units - subcutaneous at bedtime - to continue",
             "Inj Insulin Aspart 4-6 units - before each meal as per sliding scale - to continue",
             "Glucometer monitoring - 4 times daily - maintain a sugar chart"]),
    dict(slug="ureteric_stone", specialty="MS, MCh (Urology)", opd="Urology", age=(25, 60), los=(1, 3), icu=(0, 0),
         diagnosis="Right lower ureteric calculus (9 mm) with hydronephrosis",
         procedure="Right ureteroscopic lithotripsy (URSL) with DJ stenting",
         complaint="Severe right loin pain radiating to the groin for {days} days, with vomiting.",
         course=("CT KUB showed a 9 mm right lower ureteric calculus with moderate hydronephrosis. Serum creatinine was "
                 "{cr} mg/dL. URSL with laser lithotripsy and DJ stent placement was done on day 1. Pain settled and the patient "
                 "was discharged with the stent in situ, for removal after 3 weeks."),
         ecg="Pre-operative ECG (lead II) - normal sinus rhythm",
         vitals=[("Blood pressure", "142/90 mmHg", "124/80 mmHg"), ("Pulse", "100 /min", "76 /min"),
                 ("SpO2", "99 %", "99 %"), ("Temperature", "99.2 F", "98.4 F")],
         services=[("URSL procedure package", (1, 1), (45000, 70000)), ("Laser fibre and DJ stent", (1, 1), (12000, 20000)),
                   ("Operation Theatre Charges", (1, 1), (12000, 16000)), ("Anaesthesia Charges", (1, 1), (7000, 10000)),
                   ("CT KUB (plain)", (1, 1), (4500, 6500)), ("Consultant Visits", (2, 4), (800, 1200))],
         labs=[lab("hb", 12.5, 15.5, 1), lab("wbc", 7.0, 13.0, 1), lab("cr", 0.9, 1.7, 1), lab("upc", 2, 25, 0),
               lab("ua", 4.0, 9.5, 1), lab("ca", 8.6, 10.8, 1)],
         trend=("cr", "admission"),
         meds=[("Ceftriaxone", "1 g inj", 2, (1, 2), 64.00), ("Tamsulosin", "0.4 mg cap", 1, (1, 3), 12.00),
               ("Drotaverine", "80 mg tab", 2, (1, 3), 7.40), ("Paracetamol", "1 g IV infusion", 3, (1, 2), 145.00),
               ("Urology consumable kit", "kit", None, None, 9800.00)],
         rx=["Cap Tamsulosin 0.4 mg - once daily at night - 3 weeks", "Tab Cefixime 200 mg - twice daily - 5 days",
             "Drink at least 3 litres of water per day; DJ stent removal after 3 weeks"]),
]

DEFECTS = [  # each maps to behaviour already implemented in generate_packets.build/compute
    ("GRAND_TOTAL_PRINTED_WRONG_BY_1000",),
    ("UHID_BLANK",),                     # also omits the consultant signature (as in packet C)
    ("APPROVAL_PENDING_QUERY",),         # approval_status = QUERY_RAISED
]

# ---------------------------------------------------------------- helpers
def fill(fmt, rnd, when: date):
    out = fmt.replace("{yyyy}", f"{when.year}").replace("{yy}", f"{when.year % 100:02d}")
    fy = when.year if when.month >= 4 else when.year - 1                     # Indian financial year, e.g. 26-27
    out = out.replace("{fy}", f"{fy % 100:02d}-{(fy + 1) % 100:02d}")
    for n in range(10, 0, -1):
        while "{n%d}" % n in out:
            out = out.replace("{n%d}" % n, "".join(rnd.choice("0123456789") for _ in range(n)), 1)
    return out

def gstin(rnd, state):
    letters = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    pan = "AA" + "".join(rnd.choice(letters) for _ in range(3)) + "".join(rnd.choice("0123456789") for _ in range(4)) + rnd.choice(letters)
    return f"{state}{pan}1Z{rnd.choice('0123456789')}"

def sample(rnd, lo, hi, dec):
    v = rnd.uniform(lo, hi)
    return int(round(v)) if dec == 0 else round(v, dec)

def round_to(x, step):
    return float(int(round(x / step)) * step)

def trend_series(rnd, value, dec, kind, admit: date, los: int, ref_lo, ref_hi):
    """4 chart points. 'history': past tests ending with the current value (matches='last').
    'admission': starts with the admission value, then moves towards normal (matches='first')."""
    step = max(abs(value) * 0.12, 10 ** -dec if dec else 1)
    if kind == "history":
        vals = [value]
        for _ in range(3):
            vals.insert(0, max(round(vals[0] - rnd.uniform(0.3, 1.0) * step, dec), 10 ** -dec if dec else 1))
        labels = [(admit - timedelta(days=90 * (3 - i))).strftime("%b-%Y") for i in range(4)]
        matches = "last"
    else:
        if value > ref_hi:
            target = ref_hi                          # high values fall towards the upper limit
        elif value < ref_lo:
            target = ref_lo * 1.1                    # low values (platelets, bicarbonate) recover
        else:
            target = value * 0.9                     # normal values drift slightly (e.g. post-op haemoglobin)
        vals = [value] + [round(value + (target - value) * f, dec) for f in (0.35, 0.65, 0.9)]
        if los >= 3:
            offs = sorted({0, max(1, los // 3), max(2, (2 * los) // 3), los})
            while len(offs) < 4:
                offs.append(offs[-1] + 1)
            labels = [(admit + timedelta(days=o)).strftime("%d-%b") for o in offs[:4]]
        else:
            labels = ["Day 1 AM", "Day 1 PM", "Day 2 AM", "Day 2 PM"]
        matches = "first"
    if dec == 0:
        vals = [int(v) for v in vals]
    return labels, vals, matches

# ---------------------------------------------------------------- packet factory
def make_packet(i, rnd):
    h = rnd.choice(HOSPITALS)
    case = rnd.choice(CASES)
    ins = rnd.choice(INSURERS)
    gender = rnd.choice(["Male", "Female"])
    first = rnd.choice(MALE if gender == "Male" else FEMALE)
    patient = f"{first} {rnd.choice('ABCDEFGHJKLMNPRSTV') + '. ' if rnd.random() < 0.25 else ''}{rnd.choice(LAST)}"
    age = rnd.randint(*case["age"])

    admit = date(2025, 4, 1) + timedelta(days=rnd.randint(0, 560))
    los = rnd.randint(*case["los"])
    icu = min(rnd.randint(*case["icu"]), max(los - 1, 0))
    discharge = admit + timedelta(days=los)
    bill_date = discharge
    due = discharge + timedelta(days=15)

    ward_type, ward_rate = rnd.choice([("General Ward", (2500, 4800)), ("Twin Sharing", (5200, 6200)),
                                       ("Private", (6500, 9500)), ("Deluxe", (9500, 14000))])
    ward_rate = int(round_to(rnd.uniform(*ward_rate) * h["tier"], 100))

    labs, vals = [], {}
    for key, name, unit, lo, hi, charge, smin, smax, dec in case["labs"]:
        if key == "hb" and gender == "Female":
            lo, hi = 12.0, 15.5
        v = sample(rnd, smin, smax, dec)
        vals[key] = v
        labs.append((name, v, unit, lo, hi, int(round_to(charge * h["tier"], 10))))

    tkey, tkind = case["trend"]
    tname = next(l for l in case["labs"] if l[0] == tkey)
    tdec = tname[8]
    tref_lo, tref_hi = next((l[3], l[4]) for l in labs if l[0] == tname[1])
    labels, tvals, matches = trend_series(rnd, vals[tkey], tdec, tkind, admit, los, tref_lo, tref_hi)
    title = f"{tname[1]} trend (last 4 tests)" if tkind == "history" else f"{tname[1]} during admission"

    services = [(desc, rnd.randint(*q), round_to(rnd.uniform(*r) * h["tier"], 50)) for desc, q, r in case["services"]]
    meds = []
    for name, form, per_day, days, price in case["meds"]:
        meds.append((name, form, per_day, rnd.randint(*days) if days else None, round(price * h["tier"], 2)))

    # one stable consultant per hospital + specialty
    doc_name = (f"Dr. {random.Random(h['slug'] + case['opd']).choice(DOC_FIRST)} "
                f"{random.Random(case['opd'] + h['slug']).choice(LAST)}")
    consultant = f"{doc_name}, {case['specialty']}"
    reg_no = f"{h['council']}/{random.Random(h['slug'] + case['slug']).randint(1998, 2016)}/{random.Random(case['slug'] + h['slug']).randint(10000, 99999)}"

    defect = rnd.choice(DEFECTS)[0] if rnd.random() < DEFECT_RATE else None
    defects = [defect] if defect else []
    approval_status = "QUERY_RAISED" if defect == "APPROVAL_PENDING_QUERY" else "APPROVED"

    ctx = dict(vals, icu_days=icu, hrs=rnd.randint(2, 8), days=rnd.randint(1, 6), yrs=rnd.randint(2, 6))
    fu = discharge + timedelta(days=rnd.randint(7, 14))
    key_letter = rnd.choice("ABCDEFGHJKLMNPQRSTUVWXYZ")

    p = dict(
        key=key_letter, file=f"pkt_{i:04d}_{h['slug']}_{case['slug']}.pdf", theme=h["theme"], initials=h["initials"],
        hospital=h["name"], address=h["address"], gstin=gstin(rnd, h["state"]), phone=h["phone"],
        patient=patient, uhid="" if defect == "UHID_BLANK" else fill(h["uhid"], rnd, admit), age=age, gender=gender,
        ip_no=fill(h["ip"], rnd, admit), bill_no=fill(h["bill"], rnd, bill_date),
        bill_date=bill_date.isoformat(), due_date=due.isoformat(),
        admit=admit.isoformat(), admit_time=f"{rnd.randint(0, 23):02d}:{rnd.choice(['05', '15', '20', '35', '40', '50'])}",
        discharge=discharge.isoformat(), icu_days=icu, ward_type=ward_type, ward_rate=ward_rate,
        consultant=consultant, reg_no=reg_no,
        diagnosis=case["diagnosis"], procedure=case["procedure"],
        complaint=case["complaint"].format(**ctx), course=case["course"].format(**ctx),
        ecg_caption=case["ecg"], vitals=case["vitals"], services=services,
        discount_pct=float(rnd.choice([0, 0, 2, 3, 5, 10])),
        labs=labs,
        trend=dict(title=title, labels=labels, values=tvals, unit=tname[2], ref=tref_hi, matches=matches),
        meds=meds, rx=case["rx"],
        followup=f"Review in {case['opd']} OPD on {fu.strftime('%d-%b-%Y')} with repeat investigations as advised.",
        insurer=ins["name"], policy=fill(ins["policy"], rnd, admit), member_id=fill(ins["member"], rnd, admit),
        claim_type=rnd.choice(["Cashless", "Cashless", "Cashless", "Reimbursement"]),
        advance=0.0, non_payable=round_to(rnd.uniform(400, 2500), 10), copay_pct=float(rnd.choice([0, 0, 10, 20])),
        approval_ref=fill(ins["ref"], rnd, discharge), approval_status=approval_status, defects=defects,
        _case=case["slug"], _hospital=h["slug"],
    )
    # advance = 20-60% of the grand total, rounded down to 1,000 (keeps balance payable positive)
    grand = gp.compute(p)["grand"]
    p["advance"] = float(max(5000, int(grand * rnd.uniform(0.2, 0.6) / 1000) * 1000))
    return p

# ---------------------------------------------------------------- parallel build
def _init_worker():
    gp.IMG = tempfile.mkdtemp(prefix="idp_img_")     # per-process image dir: no clashes between workers

def _build(p):
    gp.build({k: v for k, v in p.items() if not k.startswith("_")})
    return p["file"]

# ---------------------------------------------------------------- ground truth (same shape as generate_packets.main)
def ground_truth(packets, dup_rows):
    q = gp.q
    docs, items, labs, meds, rules, pages, charts = [], [], [], [], [], [], []
    for p in packets:
        c = gp.compute(p)
        rec = dict(file_name=p["file"], hospital_name=p["hospital"], hospital_gstin=p["gstin"], final_bill_no=p["bill_no"],
                   ip_no=p["ip_no"], bill_date=p["bill_date"], payment_due_date=p["due_date"], patient_name=p["patient"],
                   patient_uhid=p["uhid"] or None, age=p["age"], gender=p["gender"], admission_date=p["admit"],
                   discharge_date=p["discharge"], los_days=c["los"], icu_days=p["icu_days"], ward_days=c["ward_days"],
                   primary_diagnosis=p["diagnosis"], procedure_performed=p["procedure"], consultant=p["consultant"],
                   insurer_name=p["insurer"], policy_number=p["policy"], member_id=p["member_id"], claim_type=p["claim_type"],
                   subtotal=c["subtotal"], discount=c["discount"], gst=c["gst"], grand_total_printed=c["printed_grand"],
                   grand_total_computed=c["grand"], advance_paid=p["advance"], balance_payable=c["balance"],
                   lab_total=c["lab_total"], pharmacy_total=c["pharm_total"], claimed_amount=c["claimed"],
                   non_payable=p["non_payable"], copay_amount=c["copay"], approved_amount=c["approved"],
                   patient_share=c["patient_share"], insurer_reference=p["approval_ref"], approval_status=p["approval_status"])
        docs.append((rec, p, c))
        items += [(p["file"], i["line_no"], i["description"], i["qty"], i["rate"], i["amount"]) for i in c["items"]]
        labs += [(p["file"], l["test_name"], l["value"], l["unit"], l["ref_low"], l["ref_high"], l["flag"], l["charge"]) for l in c["labs"]]
        meds += [(p["file"], m["drug"], m["form"], m["per_day"], m["days"], m["qty"], m["unit_price"], m["amount"]) for m in c["meds"]]
        def rule(name, ok): rules.append((p["file"], name, "PASS" if ok else "FAIL"))
        rule("GRAND_TOTAL_EQUALS_SUBTOTAL_MINUS_DISCOUNT_PLUS_GST", abs(c["printed_grand"] - c["grand"]) < 0.01)
        rule("LINE_ITEMS_SUM_TO_SUBTOTAL", True)
        rule("WARD_PLUS_ICU_DAYS_EQUAL_LOS", True)
        rule("BILL_LAB_LINE_EQUALS_LAB_REPORT_TOTAL", True)
        rule("BILL_PHARMACY_LINE_EQUALS_PHARMACY_TOTAL", True)
        rule("CLAIMED_EQUALS_GRAND_TOTAL", True)
        rule("PATIENT_UHID_PRESENT", bool(p["uhid"]))
        rule("INSURER_APPROVED", p["approval_status"] == "APPROVED")
        rule("CONSULTANT_SIGNATURE_PRESENT", "UHID_BLANK" not in p["defects"])
        for pg, t in enumerate(["Discharge Summary", "Hospital Bill", "Lab Report",
                                "Pharmacy Statement / Prescription", "Insurance Claim"], start=1):
            pages.append((p["file"], pg, t))
        t = p["trend"]
        charts += [(p["file"], t["title"], i + 1, lab, float(v), t["unit"]) for i, (lab, v) in enumerate(zip(t["labels"], t["values"]))]

    cols = list(docs[0][0].keys())
    types = dict(age="INT", los_days="INT", icu_days="INT", ward_days="INT")
    money = {"subtotal", "discount", "gst", "grand_total_printed", "grand_total_computed", "advance_paid", "balance_payable",
             "lab_total", "pharmacy_total", "claimed_amount", "non_payable", "copay_amount", "approved_amount", "patient_share"}
    datecols = {"bill_date", "payment_due_date", "admission_date", "discharge_date"}
    coltype = lambda k: types.get(k) or ("DECIMAL(12,2)" if k in money else "DATE" if k in datecols else "STRING")
    def values(rows): return ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in rows)

    sql = ["-- Answer key for ALL synthetic packets: the 3 originals + the bulk set (generated by generate_bulk.py)",
           "-- Replaces the gt_* tables created by 01_ground_truth.sql (it is a superset). Change the catalog to yours.",
           "USE CATALOG main;            -- <== change", "CREATE SCHEMA IF NOT EXISTS idp_test;", "USE SCHEMA idp_test;",
           "CREATE VOLUME IF NOT EXISTS raw_docs;   -- upload pdfs/ here", "",
           "CREATE OR REPLACE TABLE gt_documents (" + ", ".join(f"{k} {coltype(k)}" for k in cols) + ");",
           "INSERT INTO gt_documents VALUES\n" + ",\n".join(
               "(" + ", ".join((f"DATE {q(r[k])}" if k in datecols else q(r[k])) for k in cols) + ")" for r, _, _ in docs) + ";\n",
           "CREATE OR REPLACE TABLE gt_line_items (file_name STRING, line_no INT, description STRING, qty INT, rate DECIMAL(12,2), amount DECIMAL(12,2));",
           "INSERT INTO gt_line_items VALUES\n" + values(items) + ";\n",
           "CREATE OR REPLACE TABLE gt_lab_results (file_name STRING, test_name STRING, value DOUBLE, unit STRING, ref_low DOUBLE, ref_high DOUBLE, flag STRING, charge DECIMAL(12,2));",
           "INSERT INTO gt_lab_results VALUES\n" + values(labs) + ";\n",
           "CREATE OR REPLACE TABLE gt_medications (file_name STRING, item STRING, form STRING, doses_per_day INT, days INT, qty INT, unit_price DECIMAL(12,2), amount DECIMAL(12,2));",
           "INSERT INTO gt_medications VALUES\n" + values(meds) + ";\n",
           "CREATE OR REPLACE TABLE gt_validation (file_name STRING, rule STRING, expected STRING);",
           "INSERT INTO gt_validation VALUES\n" + values(rules) + ";\n",
           "CREATE OR REPLACE TABLE gt_page_types (file_name STRING, page_no INT, page_type STRING);",
           "INSERT INTO gt_page_types VALUES\n" + values(pages) + ";\n",
           "-- Values that appear ONLY inside the trend-chart image on page 3",
           "CREATE OR REPLACE TABLE gt_chart_values (file_name STRING, chart_title STRING, position INT, x_label STRING, value DOUBLE, unit STRING);",
           "INSERT INTO gt_chart_values VALUES\n" + values(charts) + ";\n",
           "-- Byte-identical copies: a correct pipeline skips these (same file hash as duplicate_of)",
           "CREATE OR REPLACE TABLE gt_duplicates (file_name STRING, duplicate_of STRING);",
           "INSERT INTO gt_duplicates VALUES\n" + values(dup_rows) + ";\n"]
    js = []
    for rec, p, c in docs:
        js.append(dict(rec, defects=p["defects"], line_items=c["items"], lab_results=c["labs"], medications=c["meds"],
                       trend_chart=p["trend"]))
    return "\n".join(sql) + "\n", js


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_COUNT
    rnd = random.Random(SEED)
    packets = [make_packet(i, rnd) for i in range(1, count + 1)]

    workers = max(1, min(8, (os.cpu_count() or 2) - 1))
    print(f"building {count} packets with {workers} workers ...")
    done = 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker) as ex:
        for _ in ex.map(_build, packets, chunksize=4):
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{count}")

    dup_rows = []
    for n, p in enumerate(rnd.sample(packets, N_DUPLICATES), start=1):
        name = f"upload_dup_{n}.pdf"
        shutil.copyfile(os.path.join(gp.PDF, p["file"]), os.path.join(gp.PDF, name))
        dup_rows.append((name, p["file"]))

    sql, js = ground_truth(gp.PACKETS + packets, dup_rows)
    with open(os.path.join(gp.OUT, "01b_ground_truth_bulk.sql"), "w", encoding="utf-8") as f:
        f.write(sql)
    with open(os.path.join(gp.OUT, "ground_truth_bulk.json"), "w", encoding="utf-8") as f:
        json.dump(js, f, indent=1, default=str)

    dup_of = {a: b for a, b in dup_rows}
    with open(os.path.join(gp.OUT, "bulk_manifest.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["file_name", "hospital", "case", "admission_date", "discharge_date", "claim_type",
                    "approval_status", "defects", "duplicate_of"])
        for p in packets:
            w.writerow([p["file"], p["hospital"], p["_case"], p["admit"], p["discharge"], p["claim_type"],
                        p["approval_status"], ";".join(p["defects"]), ""])
        for name, orig in dup_rows:
            w.writerow([name, "", "", "", "", "", "", "", orig])

    n_def = sum(1 for p in packets if p["defects"])
    print(f"done: {count} packets ({n_def} with a defect) + {N_DUPLICATES} duplicates -> {gp.PDF}")


if __name__ == "__main__":
    main()
