"""
Generates synthetic healthcare document packets (5 pages each) for testing
Databricks ai_parse_document / ai_classify / ai_extract and Genie Code pipelines.

All people, hospitals, insurers, policy numbers and amounts are FICTIONAL.

Each packet:
  p1 Discharge summary   (text + key-value table + vitals table + ECG image)
  p2 Final hospital bill (line-item table + totals block)
  p3 Laboratory report   (results table with flags + trend chart image)
  p4 Pharmacy statement + discharge prescription (table + Rx text + signature image)
  p5 Insurance claim summary (computation table + scanned approval letter image + stamp)

Cross-page dependencies (use them to test accuracy and validation):
  - ICU days + ward days on the bill            = length of stay on p1
  - "Laboratory Investigations" line on the bill = sum of test charges on p3
  - "Pharmacy & Consumables" line on the bill    = pharmacy statement total on p4
  - Grand total = subtotal - discount + GST (GST 5% only on ward rent > 5,000/day)
  - Claimed amount (p5) = grand total (p2); approved = (claimed - non-payable) - co-pay
  - Approved amount in the table (p5) = amount in the scanned approval letter image (p5)
  - First point of the trend chart (p3) = the value in the lab table (p3)
  - Medication qty (p4) = doses per day x days

Packet C contains deliberate defects (printed grand total is wrong by 1,000,
UHID is blank, insurer approval is pending) to test validation and null handling.

Usage:  python generate_packets.py   -> writes pdfs/, ground_truth.json, 01_ground_truth.sql
"""
import json, math, os, random
from datetime import datetime, date
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
                                Image as RLImage, PageBreak, KeepTogether)

OUT = os.path.dirname(os.path.abspath(__file__))
IMG = os.path.join(OUT, "_img")
PDF = os.path.join(OUT, "pdfs")
os.makedirs(IMG, exist_ok=True); os.makedirs(PDF, exist_ok=True)
random.seed(7)

# ---------------------------------------------------------------- helpers
def font(size, bold=False):
    for f in (("arialbd.ttf" if bold else "arial.ttf"), "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"):
        try: return ImageFont.truetype(f, size)
        except OSError: pass
    return ImageFont.load_default()

def inr(x):
    """Indian digit grouping: 190454.3 -> 1,90,454.30"""
    neg = x < 0; x = abs(round(x, 2))
    whole, frac = f"{x:.2f}".split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        parts = []
        while len(head) > 2: parts.insert(0, head[-2:]); head = head[:-2]
        if head: parts.insert(0, head)
        whole = ",".join(parts + [tail])
    return ("-" if neg else "") + whole + "." + frac

def d(s): return datetime.strptime(s, "%Y-%m-%d").date()
def dmy(s): return d(s).strftime("%d-%b-%Y")          # 06-Mar-2026
def dslash(s): return d(s).strftime("%d/%m/%Y")       # 06/03/2026 (DD/MM on purpose)
def r2(x): return round(x + 1e-9, 2)

# ---------------------------------------------------------------- images
def img_logo(path, initials, rgb):
    im = Image.new("RGBA", (360, 360), (255, 255, 255, 0)); dr = ImageDraw.Draw(im)
    for k in range(8):
        a = k * math.pi / 4
        cx, cy = 180 + 95 * math.cos(a), 180 + 95 * math.sin(a)
        dr.ellipse([cx - 60, cy - 60, cx + 60, cy + 60], fill=rgb + (70,))
    dr.ellipse([70, 70, 290, 290], fill=rgb + (255,))
    f = font(110, True); w = dr.textlength(initials, font=f)
    dr.text((180 - w / 2, 115), initials, font=f, fill="white")
    im.save(path)

def img_ecg(path, caption):
    W, H = 1800, 360; im = Image.new("RGB", (W, H), (255, 240, 240)); dr = ImageDraw.Draw(im)
    for x in range(0, W, 15): dr.line([(x, 0), (x, H)], fill=(250, 200, 200) if x % 75 else (235, 150, 150), width=1)
    for y in range(0, H, 15): dr.line([(0, y), (W, y)], fill=(250, 200, 200) if y % 75 else (235, 150, 150), width=1)
    pts, base = [], 210
    for x in range(W):
        t = x % 260; y = base
        if 30 < t < 60: y -= 18 * math.sin((t - 30) / 30 * math.pi)          # P
        elif 80 < t < 88: y += (t - 80) * 4                                  # Q
        elif 88 <= t < 100: y -= (t - 88) * 14                               # R up
        elif 100 <= t < 112: y = base - 168 + (t - 100) * 17                 # R down / S
        elif 112 <= t < 125: y = base + 36 - (t - 112) * 2.8
        elif 140 < t < 200: y -= 30 * math.sin((t - 140) / 60 * math.pi) - 8  # T (slightly depressed ST)
        pts.append((x, y + random.uniform(-1.2, 1.2)))
    dr.line(pts, fill=(20, 20, 20), width=3)
    dr.text((14, 10), caption, font=font(30, True), fill=(120, 0, 0))
    dr.text((W - 360, 10), "25 mm/s   10 mm/mV", font=font(26), fill=(120, 0, 0))
    im.save(path)

def img_trend(path, title, labels, values, unit, ref_high):
    W, H = 1400, 760; im = Image.new("RGB", (W, H), "white"); dr = ImageDraw.Draw(im)
    dr.text((40, 24), title, font=font(40, True), fill=(30, 30, 30))
    x0, y0, x1, y1 = 140, 120, W - 60, H - 120
    vmax = max(max(values), ref_high) * 1.25
    dr.line([(x0, y0), (x0, y1), (x1, y1)], fill=(60, 60, 60), width=3)
    for k in range(6):
        v = vmax * k / 5; y = y1 - (y1 - y0) * k / 5
        dr.line([(x0 - 8, y), (x1, y)], fill=(225, 225, 225), width=1)
        dr.text((20, y - 16), f"{v:.1f}", font=font(26), fill=(80, 80, 80))
    yr = y1 - (y1 - y0) * ref_high / vmax
    for xx in range(x0, x1, 24): dr.line([(xx, yr), (xx + 12, yr)], fill=(200, 40, 40), width=3)
    dr.text((W - 520, 34), f"- - -  Upper reference {ref_high} {unit}", font=font(26), fill=(200, 40, 40))
    n = len(values); slot = (x1 - x0) / n
    for i, (lab, v) in enumerate(zip(labels, values)):
        bx = x0 + slot * i + slot * 0.2; bw = slot * 0.6; by = y1 - (y1 - y0) * v / vmax
        dr.rectangle([bx, by, bx + bw, y1], fill=(37, 99, 235))
        t = f"{v}"; w = dr.textlength(t, font=font(32, True))
        dr.text((bx + bw / 2 - w / 2, by - 44), t, font=font(32, True), fill=(20, 20, 20))
        w = dr.textlength(lab, font=font(28))
        dr.text((bx + bw / 2 - w / 2, y1 + 18), lab, font=font(28), fill=(50, 50, 50))
    dr.text((x0, H - 52), f"Unit: {unit}", font=font(26), fill=(90, 90, 90))
    im.save(path)

def img_signature(path, seed):
    rnd = random.Random(seed); W, H = 700, 220
    im = Image.new("RGBA", (W, H), (255, 255, 255, 0)); dr = ImageDraw.Draw(im)
    x, y, pts = 30, 130, []
    for i in range(140):
        x += rnd.uniform(2, 6); y += rnd.uniform(-14, 14); y = min(max(y, 40), 190)
        pts.append((x, y))
    dr.line(pts, fill=(15, 40, 140, 255), width=5, joint="curve")
    dr.line([(60, 170), (W - 80, 150)], fill=(15, 40, 140, 255), width=4)
    im.save(path)

def img_stamp(path, top, mid, rgb):
    W = 520; im = Image.new("RGBA", (W, W), (255, 255, 255, 0)); dr = ImageDraw.Draw(im)
    c = rgb + (210,)
    dr.ellipse([10, 10, W - 10, W - 10], outline=c, width=12)
    dr.ellipse([60, 60, W - 60, W - 60], outline=c, width=5)
    f = font(40, True)
    for i, ch in enumerate(top):
        a = math.pi + (i + 0.5) / len(top) * math.pi
        cx, cy = W / 2 + 205 * math.cos(a), W / 2 + 205 * math.sin(a)
        dr.text((cx - 13, cy - 22), ch, font=f, fill=c)
    for j, line in enumerate(mid.split("\n")):
        w = dr.textlength(line, font=font(46, True))
        dr.text((W / 2 - w / 2, 190 + j * 62), line, font=font(46, True), fill=c)
    im = im.rotate(-14, resample=Image.BICUBIC)
    im.save(path)

def img_approval_letter(path, insurer, lines, rows, footer, seed):
    """Scanned-looking letter: text + a small table rendered INTO the image (OCR test)."""
    rnd = random.Random(seed); W, H = 1500, 1000
    im = Image.new("RGB", (W, H), (248, 246, 238)); dr = ImageDraw.Draw(im)
    dr.rectangle([0, 0, W, 110], fill=(14, 116, 144))
    dr.text((40, 30), insurer, font=font(48, True), fill="white")
    y = 140
    for ln in lines:
        dr.text((50, y), ln, font=font(30, ln.startswith("**")), fill=(25, 25, 25)) if not ln.startswith("**") else \
            dr.text((50, y), ln[2:], font=font(30, True), fill=(25, 25, 25))
        y += 46
    y += 10; cx = [50, 820, 1180]; rh = 52
    for i, row in enumerate(rows):
        dr.rectangle([50, y, W - 50, y + rh], outline=(60, 60, 60), width=2,
                     fill=(225, 232, 240) if i == 0 else None)
        for j, cell in enumerate(row):
            dr.text((cx[j] + 14, y + 11), cell, font=font(28, i == 0 or i == len(rows) - 1), fill=(20, 20, 20))
        y += rh
    dr.line([(810, y - rh * len(rows)), (810, y)], fill=(60, 60, 60), width=2)
    dr.line([(1170, y - rh * len(rows)), (1170, y)], fill=(60, 60, 60), width=2)
    dr.text((50, y + 30), footer, font=font(28), fill=(25, 25, 25))
    for _ in range(2500):  # scanner speckle
        px, py = rnd.randint(0, W - 1), rnd.randint(0, H - 1)
        dr.point((px, py), fill=(rnd.randint(120, 200),) * 3)
    im = im.rotate(1.1, resample=Image.BICUBIC, expand=True, fillcolor=(255, 255, 255))
    im = im.filter(ImageFilter.GaussianBlur(0.6))
    im.save(path, quality=80)

# ---------------------------------------------------------------- packet data (FICTIONAL)
PACKETS = [
 dict(
  key="A", file="packet_A_lotus_valley_cardiac.pdf", theme=(194, 65, 12), initials="LV",
  hospital="Lotus Valley Multispeciality Hospital", address="Plot 14, Financial District Road, Hyderabad, Telangana 500032",
  gstin="36AAACL4412F1Z9", phone="+91 40 6612 4400",
  patient="Ravi Kumar Menon", uhid="LVH-2026-004417", age=58, gender="Male",
  ip_no="IP-26-00981", bill_no="LVH/IP/2026/01893", bill_date="2026-03-06", due_date="2026-03-21",
  admit="2026-03-02", admit_time="10:40", discharge="2026-03-06", icu_days=1, ward_type="Twin Sharing", ward_rate=5500,
  consultant="Dr. Sanjana Rao, MD, DM (Cardiology)", reg_no="TSMC/2009/18822",
  diagnosis="Acute NSTEMI (non-ST elevation myocardial infarction) with Type 2 Diabetes Mellitus",
  procedure="PCI to LAD with 1 drug-eluting stent",
  complaint="Retrosternal chest pain radiating to the left arm for 3 hours, with sweating.",
  course=("ECG at admission showed ST depression in leads V4-V6. Troponin I was elevated (2.8 ng/mL). "
          "Coronary angiography on day 2 revealed 90% stenosis of the proximal LAD, treated with PCI and one "
          "drug-eluting stent. Post-procedure recovery was uneventful; the patient was shifted from ICU to the ward "
          "after 1 day. Glycaemic control was suboptimal (HbA1c 8.9%) and insulin glargine was started."),
  ecg_caption="12-lead ECG (lead V5) at admission",
  vitals=[("Blood pressure", "148/92 mmHg", "126/80 mmHg"), ("Pulse", "104 /min", "78 /min"),
          ("SpO2", "95 %", "98 %"), ("Temperature", "98.8 F", "98.4 F")],
  services=[("Cath Lab Procedure - PCI with 1 DES", 1, 145000.00), ("Consultant Visits", 8, 1200.00),
            ("Radiology - Chest X-Ray PA view", 1, 650.00)],
  discount_pct=5.0,
  labs=[("Haemoglobin", 12.1, "g/dL", 13.0, 17.0, 250), ("Total WBC count", 11.8, "x10^3/uL", 4.0, 11.0, 250),
        ("Platelet count", 245, "x10^3/uL", 150, 410, 200), ("HbA1c", 8.9, "%", 4.0, 5.6, 650),
        ("Fasting blood glucose", 162, "mg/dL", 70, 100, 150), ("LDL cholesterol", 148, "mg/dL", 0, 100, 400),
        ("Troponin I", 2.8, "ng/mL", 0, 0.04, 1200), ("Serum creatinine", 1.1, "mg/dL", 0.7, 1.3, 300)],
  trend=dict(title="HbA1c trend (last 4 tests)", labels=["Mar-2025", "Jun-2025", "Oct-2025", "Mar-2026"],
             values=[7.2, 7.8, 8.4, 8.9], unit="%", ref=5.6, matches="last"),
  meds=[("Aspirin", "75 mg tab", 1, 4, 2.50), ("Clopidogrel", "75 mg tab", 1, 4, 9.80),
        ("Atorvastatin", "80 mg tab", 1, 4, 18.00), ("Enoxaparin", "60 mg inj", 2, 2, 485.00),
        ("Metformin", "500 mg tab", 2, 4, 3.20), ("Insulin Glargine", "100 IU/mL pen", None, None, 1150.00),
        ("Cath lab consumable kit", "kit", None, None, 8600.00)],
  rx=["Tab Aspirin 75 mg - once daily after lunch - to continue",
      "Tab Clopidogrel 75 mg - once daily - 12 months",
      "Tab Atorvastatin 80 mg - once daily at night - to continue",
      "Tab Metformin 500 mg - twice daily after food - to continue",
      "Inj Insulin Glargine 10 units - subcutaneous at bedtime - review in 2 weeks"],
  followup="Review in Cardiology OPD on 20-Mar-2026 with fasting lipid profile and blood sugar chart.",
  insurer="Bharat Shield Health Insurance Ltd", policy="BSH-HLT-778812", member_id="BSH-M-0091-4471",
  claim_type="Cashless", advance=20000.00, non_payable=1840.00, copay_pct=10.0,
  approval_ref="BSH/CL/2026/558201", approval_status="APPROVED", defects=[]),
 dict(
  key="B", file="packet_B_greenfield_surgery.pdf", theme=(21, 128, 61), initials="GF",
  hospital="Greenfield Surgical & Diabetes Centre", address="22 Baner Road, Pune, Maharashtra 411045",
  gstin="27AAFCG7781K1Z2", phone="+91 20 4910 2200",
  patient="Ananya Deshpande", uhid="GSC-118-20944", age=41, gender="Female",
  ip_no="ADM/2026/4417", bill_no="GSC-FB-26-07731", bill_date="2026-04-17", due_date="2026-05-02",
  admit="2026-04-14", admit_time="08:15", discharge="2026-04-17", icu_days=0, ward_type="Private", ward_rate=7200,
  consultant="Dr. Kedar Joshi, MS (General Surgery)", reg_no="MMC/2011/40213",
  diagnosis="Acute calculous cholecystitis",
  procedure="Laparoscopic cholecystectomy",
  complaint="Right upper abdominal pain for 2 days with fever and vomiting.",
  course=("Ultrasound abdomen showed a distended gallbladder with multiple calculi and wall thickening. "
          "Liver function tests were deranged at admission (total bilirubin 2.4 mg/dL). The patient underwent "
          "laparoscopic cholecystectomy on day 1 under general anaesthesia. Bilirubin fell to 1.6 mg/dL by discharge. "
          "Recovery was uneventful and the patient tolerated an oral diet from day 2."),
  ecg_caption="Pre-operative ECG (lead II) - normal sinus rhythm",
  vitals=[("Blood pressure", "132/84 mmHg", "118/76 mmHg"), ("Pulse", "98 /min", "80 /min"),
          ("SpO2", "98 %", "99 %"), ("Temperature", "100.6 F", "98.6 F")],
  services=[("Laparoscopic Cholecystectomy - surgeon package", 1, 68000.00), ("Operation Theatre Charges", 1, 18000.00),
            ("Anaesthesia Charges", 1, 12000.00), ("Consultant Visits", 5, 1000.00),
            ("Ultrasound Abdomen", 1, 1800.00)],
  discount_pct=3.0,
  labs=[("Haemoglobin", 12.8, "g/dL", 12.0, 15.5, 250), ("Total WBC count", 13.2, "x10^3/uL", 4.0, 11.0, 250),
        ("Total bilirubin", 2.4, "mg/dL", 0.3, 1.2, 300), ("Alkaline phosphatase", 210, "U/L", 44, 147, 300),
        ("SGPT (ALT)", 68, "U/L", 7, 56, 250), ("Serum amylase", 82, "U/L", 28, 100, 450),
        ("Fasting blood glucose", 94, "mg/dL", 70, 100, 150)],
  trend=dict(title="Total bilirubin during admission", labels=["14-Apr", "15-Apr", "16-Apr", "17-Apr"],
             values=[2.4, 2.1, 1.8, 1.6], unit="mg/dL", ref=1.2, matches="first"),
  meds=[("Ceftriaxone", "1 g inj", 2, 3, 64.00), ("Pantoprazole", "40 mg inj", 1, 3, 52.00),
        ("Paracetamol", "1 g IV infusion", 3, 2, 145.00), ("Ondansetron", "4 mg inj", 2, 2, 18.50),
        ("Drotaverine", "80 mg tab", 2, 3, 7.40), ("Laparoscopy consumable kit", "kit", None, None, 14500.00)],
  rx=["Tab Cefuroxime 500 mg - twice daily - 5 days",
      "Tab Pantoprazole 40 mg - once daily before breakfast - 14 days",
      "Tab Paracetamol 650 mg - when required for pain, max 3 per day - 5 days"],
  followup="Suture removal and review in Surgery OPD on 24-Apr-2026. Histopathology report to be collected.",
  insurer="Sahyadri General Insurance Co.", policy="SGI-FAM-552019", member_id="SGI-55201-03",
  claim_type="Cashless", advance=25000.00, non_payable=1250.00, copay_pct=0.0,
  approval_ref="SGI/AUTH/26/119874", approval_status="APPROVED", defects=[]),
 dict(
  key="C", file="packet_C_riverbend_pneumonia_DEFECTS.pdf", theme=(30, 64, 175), initials="RG",
  hospital="Riverbend General Hospital", address="7 Kamarajar Salai, Mylapore, Chennai, Tamil Nadu 600004",
  gstin="33AABCR2290M1Z5", phone="+91 44 2811 9000",
  patient="Mohammed Irfan S", uhid="", age=66, gender="Male",
  ip_no="RGH-IP-55102", bill_no="RGH/2026/FB/03310", bill_date="2026-05-25", due_date="2026-06-09",
  admit="2026-05-20", admit_time="22:05", discharge="2026-05-25", icu_days=2, ward_type="General Ward", ward_rate=4800,
  consultant="Dr. Lakshmi Narayanan, MD (Pulmonology)", reg_no="TNMC/2004/77310",
  diagnosis="Community-acquired pneumonia, right lower lobe, with acute kidney injury",
  procedure="Non-invasive ventilation (BiPAP)",
  complaint="Fever, productive cough and breathlessness for 5 days.",
  course=("Chest X-ray showed right lower lobe consolidation. CRP was 96 mg/L and procalcitonin 2.1 ng/mL at admission. "
          "The patient needed BiPAP support in ICU for 2 days and IV piperacillin-tazobactam. Creatinine peaked at "
          "1.6 mg/dL and improved with hydration. CRP fell to 18 mg/L by discharge."),
  ecg_caption="ECG (lead II) at admission - sinus tachycardia",
  vitals=[("Blood pressure", "104/66 mmHg", "122/78 mmHg"), ("Pulse", "116 /min", "84 /min"),
          ("SpO2", "86 % (room air)", "96 % (room air)"), ("Temperature", "102.4 F", "98.6 F")],
  services=[("Non-invasive ventilation (BiPAP) per day", 2, 3500.00), ("Consultant Visits", 10, 900.00),
            ("Radiology - Chest X-Ray PA view (x2)", 2, 600.00), ("Physiotherapy sessions", 4, 500.00)],
  discount_pct=0.0,
  labs=[("Haemoglobin", 11.2, "g/dL", 13.0, 17.0, 250), ("Total WBC count", 16.4, "x10^3/uL", 4.0, 11.0, 250),
        ("C-reactive protein", 96, "mg/L", 0, 5, 600), ("Procalcitonin", 2.1, "ng/mL", 0, 0.5, 1800),
        ("Serum creatinine", 1.6, "mg/dL", 0.7, 1.3, 300), ("Serum sodium", 131, "mmol/L", 135, 145, 280)],
  trend=dict(title="C-reactive protein during admission", labels=["20-May", "22-May", "24-May", "25-May"],
             values=[96, 74, 41, 18], unit="mg/L", ref=5, matches="first"),
  meds=[("Piperacillin-Tazobactam", "4.5 g inj", 3, 5, 420.00), ("Azithromycin", "500 mg tab", 1, 5, 28.00),
        ("Budesonide nebulisation", "0.5 mg respule", 2, 5, 22.00), ("Paracetamol", "650 mg tab", 3, 5, 2.10),
        ("Oxygen therapy", "per hour", None, None, 2700.00)],
  rx=["Tab Amoxicillin-Clavulanate 625 mg - three times daily - 5 days",
      "Budesonide 0.5 mg nebulisation - twice daily - 7 days",
      "Tab Paracetamol 650 mg - when required for fever - 3 days"],
  followup="Review in Pulmonology OPD on 01-Jun-2026 with repeat chest X-ray and serum creatinine.",
  insurer="Coastal Assurance India Ltd", policy="CAI-SR-300471", member_id="CAI-300471-01",
  claim_type="Reimbursement", advance=30000.00, non_payable=980.00, copay_pct=20.0,
  approval_ref="CAI/QRY/2026/00412", approval_status="QUERY_RAISED",
  defects=["GRAND_TOTAL_PRINTED_WRONG_BY_1000", "UHID_BLANK", "APPROVAL_PENDING_QUERY"]),
]

# ---------------------------------------------------------------- calculations
def compute(p):
    los = (d(p["discharge"]) - d(p["admit"])).days
    ward_days = los - p["icu_days"]
    lab_total = r2(sum(l[5] for l in p["labs"]))
    meds = []
    for name, form, per_day, days, price in p["meds"]:
        qty = per_day * days if per_day else 1
        if name == "Oxygen therapy": qty = 60; price = 45.00
        meds.append(dict(drug=name, form=form, per_day=per_day, days=days, qty=qty, unit_price=price, amount=r2(qty * price)))
    pharm_total = r2(sum(m["amount"] for m in meds))
    lines = []
    if p["icu_days"]: lines.append(("ICU Charges (per day)", p["icu_days"], 12000.00))
    lines.append((f"Ward Rent - {p['ward_type']} (per day)", ward_days, float(p["ward_rate"])))
    lines += p["services"]
    lines.append(("Laboratory Investigations (see Laboratory Report)", 1, lab_total))
    lines.append(("Pharmacy & Consumables (see Pharmacy Statement)", 1, pharm_total))
    items = [dict(line_no=i + 1, description=a, qty=b, rate=c, amount=r2(b * c)) for i, (a, b, c) in enumerate(lines)]
    subtotal = r2(sum(i["amount"] for i in items))
    discount = r2((subtotal - pharm_total) * p["discount_pct"] / 100)
    ward_amt = r2(ward_days * p["ward_rate"])
    gst = r2(ward_amt * 0.05) if p["ward_rate"] > 5000 else 0.0
    grand = r2(subtotal - discount + gst)
    printed_grand = r2(grand + 1000) if "GRAND_TOTAL_PRINTED_WRONG_BY_1000" in p["defects"] else grand
    balance = r2(printed_grand - p["advance"])
    claimed = printed_grand
    admissible = r2(claimed - p["non_payable"])
    if p["approval_status"] == "APPROVED":
        copay = r2(admissible * p["copay_pct"] / 100); approved = r2(admissible - copay)
        patient_share = r2(claimed - approved); due_at_discharge = r2(patient_share - p["advance"])
    else:
        copay = approved = patient_share = due_at_discharge = None
    labs = []
    for n, v, u, lo, hi, price in p["labs"]:
        flag = "H" if v > hi else ("L" if v < lo else "N")
        labs.append(dict(test_name=n, value=v, unit=u, ref_low=lo, ref_high=hi, flag=flag, charge=price))
    return dict(los=los, ward_days=ward_days, lab_total=lab_total, meds=meds, pharm_total=pharm_total, items=items,
                subtotal=subtotal, discount=discount, gst=gst, grand=grand, printed_grand=printed_grand,
                balance=balance, claimed=claimed, admissible=admissible, copay=copay, approved=approved,
                patient_share=patient_share, due_at_discharge=due_at_discharge, labs=labs)

# ---------------------------------------------------------------- PDF
ss = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontSize=15, spaceAfter=4, spaceBefore=2)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontSize=11.5, spaceAfter=3, spaceBefore=6)
BODY = ParagraphStyle("b", parent=ss["Normal"], fontSize=9.2, leading=12.4)
SMALL = ParagraphStyle("s", parent=BODY, fontSize=7.8, leading=10, textColor=colors.HexColor("#555555"))
CELL = ParagraphStyle("c", parent=BODY, fontSize=8.6, leading=10.6)
CELLB = ParagraphStyle("cb", parent=CELL, fontName="Helvetica-Bold")

def grid(data, widths, header=True, align_right_cols=(), zebra=True, bold_last=0):
    t = Table([[Paragraph(str(c), CELLB if (header and r == 0) or (bold_last and r >= len(data) - bold_last) else CELL)
                for c in row] for r, row in enumerate(data)], colWidths=widths, repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#9a9a9a")),
          ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
          ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]
    if header: st.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5e7eb")))
    if zebra:
        for r in range(1 if header else 0, len(data)):
            if r % 2 == 0: st.append(("BACKGROUND", (0, r), (-1, r), colors.HexColor("#f7f7f7")))
    for c in align_right_cols: st.append(("ALIGN", (c, 0), (c, -1), "RIGHT"))
    t.setStyle(TableStyle(st)); return t

def kv(rows, widths):
    t = Table([[Paragraph(f"<b>{a}</b>", CELL), Paragraph(str(b), CELL), Paragraph(f"<b>{c}</b>", CELL), Paragraph(str(e), CELL)]
               for a, b, c, e in rows], colWidths=widths)
    t.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#9a9a9a")),
                           ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d0d0")),
                           ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f4f6")),
                           ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#f3f4f6")),
                           ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]))
    return t

def build(p):
    c = compute(p); k = p["key"]
    paths = {n: os.path.join(IMG, f"{k}_{n}.png") for n in ("logo", "ecg", "trend", "sig1", "sig2", "stamp", "approval")}
    img_logo(paths["logo"], p["initials"], p["theme"])
    img_ecg(paths["ecg"], p["ecg_caption"])
    t = p["trend"]; img_trend(paths["trend"], t["title"], t["labels"], t["values"], t["unit"], t["ref"])
    img_signature(paths["sig1"], seed=ord(k) * 7); img_signature(paths["sig2"], seed=ord(k) * 7 + 1)
    img_stamp(paths["stamp"], "ACCOUNTS DEPT", "PAID\nADVANCE", p["theme"])
    if p["approval_status"] == "APPROVED":
        lines = [f"Date: {dmy(p['discharge'])}", f"To: Billing Desk, {p['hospital']}",
                 f"**Subject: Final cashless authorisation - Ref {p['approval_ref']}",
                 f"Patient: {p['patient']}    Policy No: {p['policy']}    Member ID: {p['member_id']}",
                 "We are pleased to confirm final authorisation for the hospitalisation below:"]
        rows = [("Particulars", "Amount (INR)", ""), ("Amount claimed", inr(c["claimed"]), ""),
                ("Less: Non-payable items", inr(p["non_payable"]), ""),
                (f"Less: Co-payment @ {p['copay_pct']:.0f}%", inr(c["copay"]), ""),
                ("Final approved amount", inr(c["approved"]), "")]
        footer = "Balance, if any, is payable by the insured. This is a system-generated letter."
    else:
        lines = [f"Date: {dmy(p['discharge'])}", f"To: {p['patient']}",
                 f"**Subject: Query on reimbursement claim - Ref {p['approval_ref']}",
                 f"Policy No: {p['policy']}    Member ID: {p['member_id']}",
                 "Your claim is UNDER PROCESS. Please submit the following to proceed:",
                 "1. Discharge summary signed by the treating consultant (signature missing).",
                 "2. Original pharmacy bills for oxygen therapy."]
        rows = [("Particulars", "Amount (INR)", ""), ("Amount claimed", inr(c["claimed"]), ""),
                ("Approved amount", "PENDING", "")]
        footer = "No payment will be released until the query is resolved within 15 days."
    img_approval_letter(paths["approval"], p["insurer"], lines, rows, footer, seed=len(k) * 11)

    def on_page(cv, doc):
        cv.saveState(); W, H = A4
        cv.drawImage(paths["logo"], 15 * mm, H - 27 * mm, 18 * mm, 18 * mm, mask="auto")
        cv.setFont("Helvetica-Bold", 14); cv.setFillColorRGB(*[x / 255 for x in p["theme"]])
        cv.drawString(37 * mm, H - 16 * mm, p["hospital"])
        cv.setFont("Helvetica", 8); cv.setFillColor(colors.HexColor("#444444"))
        cv.drawString(37 * mm, H - 21 * mm, f"{p['address']}  |  Ph {p['phone']}  |  GSTIN {p['gstin']}")
        cv.setStrokeColorRGB(*[x / 255 for x in p["theme"]]); cv.setLineWidth(1.2)
        cv.line(15 * mm, H - 30 * mm, W - 15 * mm, H - 30 * mm)
        cv.setFont("Helvetica", 7.5); cv.setFillColor(colors.HexColor("#666666"))
        cv.drawString(15 * mm, 10 * mm, f"Patient: {p['patient']}   UHID: {p['uhid'] or '________'}   IP No: {p['ip_no']}")
        cv.drawRightString(W - 15 * mm, 10 * mm, f"Page {doc.page} of 5")
        cv.drawCentredString(W / 2, 5.5 * mm, "SYNTHETIC TEST DOCUMENT - fictional data for AI document-processing tests")
        cv.restoreState()

    S = []; FW = A4[0] - 30 * mm
    # ---------- page 1: discharge summary
    S += [Paragraph("DISCHARGE SUMMARY", H1)]
    S += [kv([("Patient name", p["patient"], "UHID", p["uhid"] or "&nbsp;"),
              ("Age / Gender", f"{p['age']} years / {p['gender']}", "IP No.", p["ip_no"]),
              ("Date of admission", f"{dslash(p['admit'])} {p['admit_time']}", "Date of discharge", dslash(p["discharge"])),
              ("Length of stay", f"{c['los']} days ({p['icu_days']} ICU + {c['ward_days']} ward)", "Treating consultant", p["consultant"]),
              ("Insurance", f"{p['insurer']} ({p['claim_type']})", "Policy No.", p["policy"])],
             [32 * mm, 58 * mm, 32 * mm, FW - 122 * mm])]
    S += [Paragraph("Final diagnosis", H2), Paragraph(p["diagnosis"], BODY),
          Paragraph("Procedure performed", H2), Paragraph(p["procedure"], BODY),
          Paragraph("Presenting complaint", H2), Paragraph(p["complaint"], BODY),
          Paragraph("Course in hospital", H2), Paragraph(p["course"], BODY), Spacer(1, 4),
          RLImage(paths["ecg"], width=FW, height=FW * 360 / 1800),
          Paragraph(f"Figure 1: {p['ecg_caption']}", SMALL),
          Paragraph("Vital signs", H2),
          grid([("Parameter", "At admission", "At discharge")] + p["vitals"], [50 * mm, 50 * mm, 50 * mm]),
          Paragraph("Follow-up", H2), Paragraph(p["followup"], BODY)]
    if "UHID_BLANK" not in p["defects"]:
        S += [Spacer(1, 4), Table([[RLImage(paths["sig1"], 38 * mm, 12 * mm)], [Paragraph(f"{p['consultant']}<br/>Reg. No. {p['reg_no']}", SMALL)]],
                                  colWidths=[80 * mm], hAlign="RIGHT")]
    else:
        S += [Spacer(1, 6), Paragraph(f"Consultant signature: ______________________ &nbsp; ({p['consultant']})", SMALL)]
    S += [PageBreak()]
    # ---------- page 2: final bill
    S += [Paragraph("FINAL HOSPITAL BILL (IN-PATIENT)", H1),
          kv([("Final Bill No.", p["bill_no"], "Bill Date", dmy(p["bill_date"])),
              ("IP No.", p["ip_no"], "Payment Due By", dmy(p["due_date"])),
              ("Bill To", f"{p['patient']} / {p['insurer']}", "Bill Type", f"{p['claim_type']} - Final")],
             [30 * mm, 62 * mm, 32 * mm, FW - 124 * mm]), Spacer(1, 6)]
    rows = [("#", "Description of service", "Qty", "Rate (INR)", "Amount (INR)")]
    rows += [(i["line_no"], i["description"], i["qty"], inr(i["rate"]), inr(i["amount"])) for i in c["items"]]
    S += [grid(rows, [8 * mm, 98 * mm, 14 * mm, 28 * mm, FW - 148 * mm], align_right_cols=(2, 3, 4))]
    tot = [("Sub-total", inr(c["subtotal"])),
           (f"Less: Corporate discount @ {p['discount_pct']:.0f}% on hospital services (excl. pharmacy)", inr(c["discount"])),
           ("Add: GST @ 5% on ward rent (rent above INR 5,000 per day)" if c["gst"] else "Add: GST (not applicable - ward rent up to INR 5,000 per day)", inr(c["gst"])),
           ("GRAND TOTAL", inr(c["printed_grand"])),
           ("Less: Advance received", inr(p["advance"])),
           ("Balance payable", inr(c["balance"]))]
    S += [Spacer(1, 4), grid(tot, [FW - 40 * mm, 40 * mm], header=False, align_right_cols=(1,), zebra=False, bold_last=3)]
    S += [Spacer(1, 6), Paragraph(
        "Notes: (1) Ward rent is charged per midnight stay; ICU days are billed separately. (2) Laboratory and pharmacy "
        "amounts are carried forward from the attached Laboratory Report and Pharmacy Statement. (3) Healthcare services are "
        "exempt from GST except room rent above INR 5,000 per day. (4) Please quote the Final Bill No. in all correspondence.", SMALL),
        Spacer(1, 6),
        Table([[RLImage(paths["stamp"], 30 * mm, 30 * mm), "", RLImage(paths["sig2"], 38 * mm, 12 * mm)],
               ["", "", Paragraph("Authorised signatory - Billing", SMALL)]], colWidths=[40 * mm, FW - 100 * mm, 60 * mm]),
        PageBreak()]
    # ---------- page 3: lab report
    S += [Paragraph("LABORATORY REPORT", H1),
          kv([("Sample collected", f"{dslash(p['admit'])} {p['admit_time']}", "Reported", dslash(p["admit"])),
              ("Referred by", p["consultant"], "Lab Ref.", f"LAB-{p['key']}-{d(p['admit']).strftime('%y%m%d')}-07")],
             [30 * mm, 62 * mm, 26 * mm, FW - 118 * mm]), Spacer(1, 6)]
    rows = [("Test", "Result", "Unit", "Reference range", "Flag", "Charge (INR)")]
    for l in c["labs"]:
        rr = f"< {l['ref_high']}" if l["ref_low"] == 0 else f"{l['ref_low']} - {l['ref_high']}"
        rows.append((l["test_name"], f"<b>{l['value']}</b>" if l["flag"] != "N" else l["value"], l["unit"], rr,
                     {"H": "HIGH", "L": "LOW", "N": ""}[l["flag"]], inr(l["charge"])))
    rows.append(("Total laboratory charges", "", "", "", "", inr(c["lab_total"])))
    S += [grid(rows, [46 * mm, 20 * mm, 24 * mm, 32 * mm, 16 * mm, FW - 138 * mm], align_right_cols=(1, 5), bold_last=1)]
    S += [Spacer(1, 8), RLImage(paths["trend"], width=FW * 0.82, height=FW * 0.82 * 760 / 1400),
          Paragraph(f"Figure 2: {t['title']}. Values in {t['unit']}; dashed line is the upper reference limit.", SMALL),
          Spacer(1, 6), Paragraph("Interpretation: values flagged HIGH or LOW are outside the laboratory reference range "
                                  "and should be correlated clinically. Reference ranges are adult values.", SMALL),
          PageBreak()]
    # ---------- page 4: pharmacy + prescription
    S += [Paragraph("PHARMACY STATEMENT", H1)]
    rows = [("Item", "Form / strength", "Doses per day", "Days", "Qty", "Unit price (INR)", "Amount (INR)")]
    for m in c["meds"]:
        rows.append((m["drug"], m["form"], m["per_day"] or "-", m["days"] or "-", m["qty"], inr(m["unit_price"]), inr(m["amount"])))
    rows.append(("Pharmacy total (carried to Final Bill)", "", "", "", "", "", inr(c["pharm_total"])))
    S += [grid(rows, [44 * mm, 30 * mm, 20 * mm, 12 * mm, 12 * mm, 26 * mm, FW - 144 * mm], align_right_cols=(2, 3, 4, 5, 6), bold_last=1)]
    S += [Paragraph("DISCHARGE PRESCRIPTION (Rx)", H2)]
    S += [Paragraph(f"{i + 1}. {r}", BODY) for i, r in enumerate(p["rx"])]
    S += [Spacer(1, 6), Paragraph("Advice: low-salt, low-fat diet; avoid strenuous activity for 2 weeks; return immediately if "
                                  "chest pain, breathlessness, fever or bleeding occurs.", BODY)]
    if "UHID_BLANK" not in p["defects"]:
        S += [Spacer(1, 8), Table([[RLImage(paths["sig1"], 38 * mm, 12 * mm)], [Paragraph(f"{p['consultant']}<br/>Reg. No. {p['reg_no']}", SMALL)]],
                                  colWidths=[80 * mm], hAlign="RIGHT")]
    else:
        S += [Spacer(1, 8), Paragraph(f"Prescribed by: {p['consultant']}, Reg. No. {p['reg_no']}", SMALL)]
    S += [PageBreak()]
    # ---------- page 5: claim summary
    S += [Paragraph("INSURANCE CLAIM SUMMARY", H1),
          kv([("Insurer", p["insurer"], "Claim type", p["claim_type"]),
              ("Policy No.", p["policy"], "Member ID", p["member_id"]),
              ("Final Bill No.", p["bill_no"], "Insurer reference", p["approval_ref"])],
             [28 * mm, 64 * mm, 30 * mm, FW - 122 * mm]), Spacer(1, 6)]
    if p["approval_status"] == "APPROVED":
        rows = [("Claim computation", "Amount (INR)"), ("Amount claimed (= Grand Total of Final Bill)", inr(c["claimed"])),
                ("Less: Non-payable items (admission kit, toiletries, attendant diet)", inr(p["non_payable"])),
                ("Admissible amount", inr(c["admissible"])),
                (f"Less: Co-payment @ {p['copay_pct']:.0f}% of admissible amount", inr(c["copay"])),
                ("Approved by insurer", inr(c["approved"])),
                ("Patient share (claimed - approved)", inr(c["patient_share"])),
                ("Less: Advance already paid by patient", inr(p["advance"])),
                ("Amount due from patient at discharge" if c["due_at_discharge"] >= 0 else "Refund due to patient",
                 inr(abs(c["due_at_discharge"])))]
    else:
        rows = [("Claim computation", "Amount (INR)"), ("Amount claimed (= Grand Total of Final Bill)", inr(c["claimed"])),
                ("Less: Non-payable items (admission kit, toiletries, attendant diet)", inr(p["non_payable"])),
                ("Admissible amount", inr(c["admissible"])),
                (f"Co-payment @ {p['copay_pct']:.0f}% (applies on approval)", "to be computed"),
                ("Approved by insurer", "PENDING - query raised"),
                ("Amount paid by patient at discharge (reimbursement claim)", inr(c["printed_grand"]))]
    S += [grid(rows, [FW - 45 * mm, 45 * mm], align_right_cols=(1,), bold_last=1)]
    S += [Spacer(1, 6), Paragraph("Insurer communication (scanned copy attached below)", H2),
          RLImage(paths["approval"], width=FW * 0.9, height=FW * 0.9 * Image.open(paths["approval"]).size[1] / Image.open(paths["approval"]).size[0]),
          Paragraph("Figure 3: Insurer letter (scanned).", SMALL)]
    doc = SimpleDocTemplate(os.path.join(PDF, p["file"]), pagesize=A4, leftMargin=15 * mm, rightMargin=15 * mm,
                            topMargin=34 * mm, bottomMargin=16 * mm, title=f"{p['hospital']} - IP packet {p['ip_no']}",
                            author="Synthetic test data")
    doc.build(S, onFirstPage=on_page, onLaterPages=on_page)
    return c

# ---------------------------------------------------------------- ground truth
def q(v):
    if v is None or v == "": return "NULL"
    if isinstance(v, (int, float)): return repr(round(v, 2)) if isinstance(v, float) else str(v)
    return "'" + str(v).replace("'", "''") + "'"

def main():
    gt, sql = [], ["-- Ground truth for the synthetic IDP test packets (generated by generate_packets.py)",
                   "-- Run in a Databricks SQL editor or notebook. Change the catalog/schema to yours.",
                   "USE CATALOG main;            -- <== change", "CREATE SCHEMA IF NOT EXISTS idp_test;", "USE SCHEMA idp_test;",
                   "CREATE VOLUME IF NOT EXISTS raw_docs;   -- upload the PDFs here", ""]
    docs, items, labs, meds, rules = [], [], [], [], []
    for p in PACKETS:
        c = build(p)
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
                   patient_share=c["patient_share"], insurer_reference=p["approval_ref"], approval_status=p["approval_status"],
                   defects=p["defects"], line_items=c["items"], lab_results=c["labs"], medications=c["meds"],
                   page_types={"1": "Discharge Summary", "2": "Hospital Bill", "3": "Lab Report",
                               "4": "Pharmacy Statement / Prescription", "5": "Insurance Claim"})
        gt.append(rec)
        docs.append(rec)
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

    cols_doc = [k for k in docs[0] if k not in ("defects", "line_items", "lab_results", "medications", "page_types")]
    types = dict(age="INT", los_days="INT", icu_days="INT", ward_days="INT")
    money = {"subtotal", "discount", "gst", "grand_total_printed", "grand_total_computed", "advance_paid", "balance_payable",
             "lab_total", "pharmacy_total", "claimed_amount", "non_payable", "copay_amount", "approved_amount", "patient_share"}
    datecols = {"bill_date", "payment_due_date", "admission_date", "discharge_date"}
    def coltype(k): return types.get(k) or ("DECIMAL(12,2)" if k in money else "DATE" if k in datecols else "STRING")
    sql.append("CREATE OR REPLACE TABLE gt_documents (" + ", ".join(f"{k} {coltype(k)}" for k in cols_doc) + ");")
    sql.append("INSERT INTO gt_documents VALUES")
    sql.append(",\n".join("(" + ", ".join((f"DATE {q(r[k])}" if k in datecols else q(r[k])) for k in cols_doc) + ")" for r in docs) + ";\n")
    sql.append("CREATE OR REPLACE TABLE gt_line_items (file_name STRING, line_no INT, description STRING, qty INT, rate DECIMAL(12,2), amount DECIMAL(12,2));")
    sql.append("INSERT INTO gt_line_items VALUES\n" + ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in items) + ";\n")
    sql.append("CREATE OR REPLACE TABLE gt_lab_results (file_name STRING, test_name STRING, value DOUBLE, unit STRING, ref_low DOUBLE, ref_high DOUBLE, flag STRING, charge DECIMAL(12,2));")
    sql.append("INSERT INTO gt_lab_results VALUES\n" + ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in labs) + ";\n")
    sql.append("CREATE OR REPLACE TABLE gt_medications (file_name STRING, item STRING, form STRING, doses_per_day INT, days INT, qty INT, unit_price DECIMAL(12,2), amount DECIMAL(12,2));")
    sql.append("INSERT INTO gt_medications VALUES\n" + ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in meds) + ";\n")
    sql.append("CREATE OR REPLACE TABLE gt_validation (file_name STRING, rule STRING, expected STRING);")
    sql.append("INSERT INTO gt_validation VALUES\n" + ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in rules) + ";\n")
    sql.append("CREATE OR REPLACE TABLE gt_page_types (file_name STRING, page_no INT, page_type STRING);")
    sql.append("INSERT INTO gt_page_types VALUES\n" + ",\n".join(
        f"({q(r['file_name'])}, {pg}, {q(t)})" for r in docs for pg, t in r["page_types"].items()) + ";\n")
    sql.append("-- Values that appear ONLY inside the trend-chart image on page 3 (tests figure understanding)")
    sql.append("CREATE OR REPLACE TABLE gt_chart_values (file_name STRING, chart_title STRING, position INT, x_label STRING, value DOUBLE, unit STRING);")
    sql.append("INSERT INTO gt_chart_values VALUES\n" + ",\n".join(
        f"({q(p['file'])}, {q(p['trend']['title'])}, {i + 1}, {q(lab)}, {q(float(v))}, {q(p['trend']['unit'])})"
        for p in PACKETS for i, (lab, v) in enumerate(zip(p["trend"]["labels"], p["trend"]["values"]))) + ";")
    with open(os.path.join(OUT, "01_ground_truth.sql"), "w", encoding="utf-8") as f: f.write("\n".join(sql) + "\n")
    with open(os.path.join(OUT, "ground_truth.json"), "w", encoding="utf-8") as f: json.dump(gt, f, indent=2, default=str)
    for r in gt:
        print(f"{r['file_name']}: LOS {r['los_days']}, subtotal {r['subtotal']}, grand printed {r['grand_total_printed']} "
              f"(computed {r['grand_total_computed']}), approved {r['approved_amount']}")

if __name__ == "__main__":
    main()
