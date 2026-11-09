# IDP test kit: test Databricks document AI with Genie Code

This kit contains three synthetic 5-page hospital in-patient packets, an answer key, and the steps to build and evaluate a document-processing pipeline with Genie Code. All names, hospitals, insurers, policy numbers and amounts are fictional.

```
idp_test_kit/
├── pdfs/
│   ├── packet_A_lotus_valley_cardiac.pdf          <- main test file, clean
│   ├── packet_B_greenfield_surgery.pdf            <- different hospital, procedure, layout values
│   └── packet_C_riverbend_pneumonia_DEFECTS.pdf   <- deliberate errors, to test validation
├── 01_ground_truth.sql      <- answer key tables (gt_*), plus schema and volume
├── 02_pipeline_and_eval.sql <- manual pipeline + evaluation (fallback, or to compare with Genie)
├── ground_truth.json        <- the same answer key as JSON
└── generate_packets.py      <- regenerates everything (python generate_packets.py; needs reportlab + Pillow)
```

## 1. What's inside each PDF

| Page | Content | Text | Tables | Images |
|---|---|---|---|---|
| 1 | Discharge summary | Diagnosis, procedure, complaint, hospital course, follow-up | Patient key-value table, vital signs | Hospital logo, **ECG strip**, consultant signature |
| 2 | Final hospital bill | Billing notes | **Line items** (qty × rate), totals block | Logo, **"PAID ADVANCE" stamp**, signature |
| 3 | Laboratory report | Interpretation note | **Results** with ranges, flags and charges | **Bar chart** of a lab trend (values exist only in the image) |
| 4 | Pharmacy statement and discharge prescription | Prescription (Rx) list, advice | **Medicines** (doses/day × days = qty) | Signature |
| 5 | Insurance claim summary | Insurer details | **Claim computation** | **Scanned insurer letter** (rotated, speckled, with a table inside the image) |

### The planted dependencies (how accuracy gets tested)

These relationships hold in the data, so a correct extraction must satisfy them:

1. **ICU days + ward days on the bill (p2) = length of stay** from admission and discharge dates (p1).
2. **Bill line "Laboratory Investigations" (p2) = total laboratory charges (p3).**
3. **Bill line "Pharmacy & Consumables" (p2) = pharmacy total (p4).**
4. **Line items sum to the sub-total.** Grand total = sub-total − discount + GST.
   - The discount is a percentage of the services *excluding pharmacy*.
   - GST is 5% of ward rent, and only when the rate is above ₹5,000/day. Packet C's ward is ₹4,800, so GST is 0.
5. **Claimed amount (p5) = grand total (p2).** Approved = (claimed − non-payable) − co-pay.
6. **The approved amount in the p5 table = the amount inside the scanned letter image.**
7. **Lab flags:** HIGH or LOW follows from the value and the reference range.
8. **The chart's first (B, C) or last (A) bar = the value in the lab table.**
9. **Medicine qty = doses per day × days.**

### Traps (where extraction usually goes wrong)

| Trap | Where | Correct answer |
|---|---|---|
| Final Bill No. vs IP No. | p2 header (both printed) | `final_bill_no` = `LVH/IP/2026/01893`, not `IP-26-00981` |
| Bill Date vs Payment Due By | p2 | Bill date `2026-03-06`, due `2026-03-21` |
| DD/MM dates | p1 `02/03/2026` | 2 **March**, not 3 February |
| Indian digit grouping | `1,90,454.30` | 190454.30 |
| Sub-total vs grand total vs balance payable | p2 totals block | Three different numbers |
| GST "not applicable" | Packet C p2 | `0.00`, not null |
| Range written as `< 100` | LDL, Troponin | ref_low 0, ref_high 100 |
| Value only inside an image | Chart (p3), insurer letter (p5) | See `gt_chart_values` |
| **Blank UHID** | Packet C | `NULL`. The model must not invent it or copy the IP No. |
| **Grand total printed wrong (+₹1,000)** | Packet C p2 | Extract the *printed* 71,471.50. Validation must flag it (computed total is 70,471.50). |
| **Approval pending** | Packet C p5 | `approved_amount` NULL, status `QUERY_RAISED` |
| **Missing consultant signature** | Packet C p1 and p4 | Signature rule should FAIL |

Expected validation result: **A and B pass every rule**. **C** fails grand-total arithmetic, UHID present, insurer approved and consultant signature present (see `gt_validation`).

## 2. Set up in Databricks (about 10 minutes)

**Prerequisites:**
- A Unity Catalog workspace in a region with AI Functions.
- **Serverless** compute or a **Pro/Serverless SQL warehouse** (not Classic).
- Genie Code agent mode enabled.
- Permission to create a schema and a volume.

1. **Create the answer key.** Open a SQL editor, paste `01_ground_truth.sql`, change `USE CATALOG main;` to your catalog, and run all. This creates the schema `idp_test`, the volume `raw_docs`, and the tables `gt_documents`, `gt_line_items`, `gt_lab_results`, `gt_medications`, `gt_validation`, `gt_page_types` and `gt_chart_values`.
2. **Upload the PDFs.** Go to Catalog Explorer → `<catalog>.idp_test.raw_docs` → **Upload to this volume**, and select the three PDFs.
   Or use the CLI: `databricks fs cp -r pdfs/ dbfs:/Volumes/<catalog>/idp_test/raw_docs/`
3. **Check the upload.** `LIST '/Volumes/<catalog>/idp_test/raw_docs/'` should show 3 files.

## 3. Build the pipeline with Genie Code: prompt by prompt

Open **Genie Code** from **Jobs & Pipelines** or the **SQL editor**. Send the prompts one at a time, and **review what it generates before approving each step**. Replace `main` with your catalog. The `@` references let Genie see your objects.

### Prompt 1: build

```
In-patient document packets (PDF, 5 pages each) land in @main.idp_test.raw_docs.
Each packet has: p1 discharge summary, p2 final hospital bill, p3 lab report (with a trend chart image),
p4 pharmacy statement + prescription, p5 insurance claim summary (with a scanned insurer letter image).

Build a Lakeflow job that:
1. Discovers new PDFs in the volume and tracks file identity (hash) so files are processed once.
2. Parses each PDF with ai_parse_document (save page images to a separate volume, not raw_docs).
3. Classifies each PAGE into: Discharge Summary, Hospital Bill, Lab Report,
   Pharmacy Statement / Prescription, Insurance Claim, Other.
4. Extracts per packet: hospital_name, final_bill_no, ip_no, bill_date, payment_due_date, patient_name,
   patient_uhid, age, gender, admission_date, discharge_date, primary_diagnosis, procedure_performed,
   insurer_name, policy_number, claim_type, subtotal, discount, gst, grand_total_printed, advance_paid,
   balance_payable, lab_total, pharmacy_total, claimed_amount, approved_amount, insurer_reference,
   approval_status, plus arrays line_items, lab_results and trend_chart_values. Include confidence scores and citations.
5. Writes results to tables in main.idp_test with a silver_ prefix.
Start by running on the 3 files in the volume.
```

**What to check in its answer:**
- Separate tasks for parse, classify, extract and publish.
- `ai_parse_document` reads `binaryFile`.
- `ai_extract` uses a JSON schema with types.

### Prompt 2: understand it

```
Explain what each task in this job does, which table each task writes, and which task I should re-run
if I only change the extraction schema.
```

### Prompt 3: run it, then debug visually

```
Run the job on the 3 files. Then show me the parsed results for packet_A_lotus_valley_cardiac.pdf
next to the original pages so I can check the parsing of the bill table and the scanned insurer letter.
```

This opens the **Document Parsing UI**. Check three things:
- The p2 line-item table came through as a table.
- The p5 scanned letter's text was read.
- The p3 chart has a figure description that mentions the bar values.

### Prompt 4: evaluate against the answer key

```
Evaluate the extraction against my labeled table @main.idp_test.gt_documents, matching on file_name.
Use exact match for IDs, date equality for dates, and exact match to 2 decimals for amounts.
Treat NULL = NULL as correct. Show per-field accuracy and list every mismatch with the extracted
and expected value.
```

Then do the same for the tables:

```
Also evaluate line items against @main.idp_test.gt_line_items (match on file_name + amount),
lab results against @main.idp_test.gt_lab_results (match on file_name + test name),
and trend_chart_values against @main.idp_test.gt_chart_values (match on file_name + position).
```

### Prompt 5: hill climb the weak fields

Pick the worst field from the evaluation. Typical examples:

```
Accuracy on final_bill_no could be higher. It sometimes picks up the IP No. instead.
```
```
admission_date is wrong for some packets. The source uses DD/MM/YYYY (Indian format).
```
```
patient_uhid should be NULL when the UHID is blank. Packet C is returning a value.
```

Approve the **description diff** Genie proposes, then let it re-run the evaluation. Note the score before and after, **and check for regressions** in other fields.

### Prompt 6: add validation using the dependencies

```
Add a validation task. Keep the rows that fail, with a reason code per rule:
- line_items amounts sum to subtotal (tolerance 1 rupee)
- grand_total_printed = subtotal - discount + gst
- ICU days + ward days from the line items = datediff(discharge_date, admission_date)
- bill line 'Laboratory Investigations' = lab_total; bill line 'Pharmacy & Consumables' = pharmacy_total
- claimed_amount = grand_total_printed
- patient_uhid is not null
- approval_status = 'APPROVED' and approved_amount is not null
- the consultant signature is present on the discharge summary page
Route failing packets and any field with confidence below 0.8 to a table main.idp_test.review_queue.
```

### Prompt 7: check the validation against the answer key

```
Compare the validation results with @main.idp_test.gt_validation (file_name, rule, expected PASS/FAIL)
and list any rule where our result disagrees with the expected result.
```

The target is: A and B pass everything, and C fails 4 rules.

### Prompt 8: production touches (optional)

```
Set confidence thresholds per field from the evaluation results.
Trigger the job on file arrival in @main.idp_test.raw_docs, retry each task twice,
and review this pipeline design against best practices without changing anything.
```

### Re-test incremental processing

Run `python generate_packets.py` again after changing a value in `PACKETS`, or copy a PDF under a new name, and upload it.
- A new file should be processed **alone**.
- An identical file under a new name should be **skipped as a duplicate** (same hash).

## 4. If Genie Code isn't available

Run `02_pipeline_and_eval.sql` top to bottom. It is the same pipeline written by hand: parse, pages, page classification, extraction, flattening, field evaluation, table evaluation, validation, and review/gold.

Run **step 4a** and look at the raw `ex:response` before trusting the flattening. The array-flattening queries accept two possible output shapes because the exact nesting of array items should be confirmed on your workspace's output.

## 5. Scorecard (fill in as you test)

| Metric | Run 1 | After hill climb | Notes |
|---|---|---|---|
| Page classification accuracy (15 pages) | | | |
| Header field accuracy (23 fields × 3) | | | |
| Line-item recall | | | |
| Lab value accuracy | | | |
| Chart values (image only) | | | |
| Approved amount from scanned letter | | | |
| Validation agreement with `gt_validation` | | | |
| Packet C: UHID returned as NULL? | | | |

**Reading the results:**
- **Garbled or missing text** in the Document Parsing UI means a parsing problem.
- **Correct text but a wrong value** means an extraction problem. Fix the field description.
- **A wrong chart value** shows the limit of figure understanding. Don't rely on values that exist only in images without a review step.
