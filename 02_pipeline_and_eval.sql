-- =====================================================================================
-- Manual pipeline + evaluation for the synthetic IDP test packets
-- Use it (a) if Genie Code is not available, or (b) to compare against what Genie builds.
--
-- Requires: serverless SQL warehouse / serverless notebook, or DBR 15.4+ (18.2+ recommended),
--           in a region where AI Functions are available. Not supported on Classic SQL warehouses.
-- Prereq:   01_ground_truth.sql has been run, and the 3 PDFs are uploaded to
--           /Volumes/main/idp_test/raw_docs/
-- Cost:     3 PDFs x 5 pages -> a few cents. Every AI call is billed, so don't loop this on big folders.
-- Change `main` below if you used another catalog.
-- =====================================================================================
USE CATALOG main;
USE SCHEMA idp_test;
CREATE VOLUME IF NOT EXISTS page_images;     -- keep rendered page images OUT of raw_docs

-- -------------------------------------------------------------------------------------
-- STEP 1  PARSE  (ai_parse_document, run ONCE and persist)
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE bronze_parsed AS
SELECT
  regexp_extract(path, '[^/]+$', 0)  AS file_name,
  path,
  sha2(content, 256)                 AS sha256,
  ai_parse_document(
    content,
    map('version', '2.0',
        'imageOutputPath', '/Volumes/main/idp_test/page_images/',
        'descriptionElementTypes', '*')) AS parsed,
  current_timestamp()                AS parsed_at
FROM READ_FILES('/Volumes/main/idp_test/raw_docs/', format => 'binaryFile')
WHERE path ILIKE '%.pdf';

-- 1a. Did parsing report errors? (expect 0 rows)
SELECT file_name, parsed:error_status FROM bronze_parsed
WHERE size(cast(parsed:error_status AS ARRAY<VARIANT>)) > 0;

-- 1b. What element types were detected?  Expect table, text, figure, title/section_header,
--     page_header/page_footer. Each packet has 4-6 figures (logo, ECG, chart, signature, stamp, letter).
SELECT file_name, e.value:type::string AS element_type, count(*) AS n
FROM bronze_parsed, LATERAL variant_explode(parsed:document.elements) AS e
GROUP BY ALL ORDER BY file_name, n DESC;

-- 1c. AI descriptions of figures: does the chart description contain the 4 bar values?
SELECT file_name, e.value:bbox[0].page_id::int AS page_id, e.value:description::string AS figure_description
FROM bronze_parsed, LATERAL variant_explode(parsed:document.elements) AS e
WHERE e.value:type::string = 'figure';

-- -------------------------------------------------------------------------------------
-- STEP 2  ONE ROW PER PAGE (text in reading order)
-- page_id from the parser: check below whether it starts at 0 or 1 and adjust page_no.
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver_pages AS
SELECT file_name,
       e.value:bbox[0].page_id::int AS page_id,
       concat_ws('\n',
         transform(array_sort(collect_list(named_struct('id', e.value:id::int,
                                                        'c',  e.value:content::string))),
                   x -> x.c)) AS page_text
FROM bronze_parsed, LATERAL variant_explode(parsed:document.elements) AS e
WHERE e.value:content IS NOT NULL
GROUP BY ALL;

SELECT file_name, min(page_id) AS first_page_id, max(page_id) AS last_page_id, count(*) AS pages
FROM silver_pages GROUP BY file_name;            -- expect 5 pages each (ids 0..4 or 1..5)

-- -------------------------------------------------------------------------------------
-- STEP 3  CLASSIFY EACH PAGE  (ai_classify v2.1)
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver_page_class AS
SELECT file_name, page_id,
  ai_classify(page_text,
    '{"Discharge Summary":"Clinical summary of an inpatient stay: diagnosis, procedure, course, vitals, follow-up",
      "Hospital Bill":"Final itemised hospital bill with line items, sub-total, discount, GST and grand total",
      "Lab Report":"Laboratory test results with values, units, reference ranges and flags",
      "Pharmacy Statement / Prescription":"Medicines dispensed with quantities and prices, and/or discharge prescription",
      "Insurance Claim":"Insurance claim computation, insurer approval or query letter",
      "Other":"Anything else"}',
    map('version', '2.1', 'enableConfidenceScores', 'true')) AS cls
FROM silver_pages;

-- Page classification accuracy  (page_no = page_id + 1 if ids start at 0)
WITH off AS (SELECT 1 - min(page_id) AS o FROM silver_pages)
SELECT round(avg(CASE WHEN c.cls:response[0].value::string = g.page_type THEN 1.0 ELSE 0.0 END), 3) AS page_type_accuracy,
       collect_list(CASE WHEN c.cls:response[0].value::string <> g.page_type
                         THEN concat(c.file_name, ' p', g.page_no, ': got ', c.cls:response[0].value::string) END) AS misses
FROM silver_page_class c
CROSS JOIN off
JOIN gt_page_types g
  ON c.file_name = g.file_name
 AND g.page_no = c.page_id + off.o;

-- -------------------------------------------------------------------------------------
-- STEP 4  EXTRACT  (ai_extract v2.1 on the whole parsed document)
-- Field descriptions deliberately warn about the traps in the packets.
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver_extract AS
SELECT file_name,
  ai_extract(parsed, '{
    "hospital_name":      {"type":"string"},
    "hospital_gstin":     {"type":"string"},
    "final_bill_no":      {"type":"string","description":"The Final Bill No. on the hospital bill. NOT the IP No."},
    "ip_no":              {"type":"string","description":"In-patient / admission number (IP No.)"},
    "bill_date":          {"type":"string","description":"Bill Date as YYYY-MM-DD. NOT the payment due date"},
    "payment_due_date":   {"type":"string","description":"Payment Due By date as YYYY-MM-DD"},
    "patient_name":       {"type":"string"},
    "patient_uhid":       {"type":"string","description":"Hospital UHID. Return null if blank or not printed; never use the IP No."},
    "age":                {"type":"integer"},
    "gender":             {"type":"string"},
    "admission_date":     {"type":"string","description":"YYYY-MM-DD. Source dates are DD/MM/YYYY (Indian format)"},
    "discharge_date":     {"type":"string","description":"YYYY-MM-DD. Source dates are DD/MM/YYYY (Indian format)"},
    "primary_diagnosis":  {"type":"string"},
    "procedure_performed":{"type":"string"},
    "insurer_name":       {"type":"string"},
    "policy_number":      {"type":"string"},
    "claim_type":         {"type":"string","description":"Cashless or Reimbursement"},
    "subtotal":           {"type":"number","description":"Bill sub-total in INR before discount and GST. Amounts use Indian grouping, e.g. 1,98,986.80 = 198986.80"},
    "discount":           {"type":"number"},
    "gst":                {"type":"number","description":"GST amount on the bill; 0 if shown as 0.00 / not applicable"},
    "grand_total_printed":{"type":"number","description":"GRAND TOTAL exactly as printed on the bill (do not recompute)"},
    "advance_paid":       {"type":"number"},
    "balance_payable":    {"type":"number"},
    "lab_total":          {"type":"number","description":"Total laboratory charges on the Laboratory Report"},
    "pharmacy_total":     {"type":"number","description":"Pharmacy total on the Pharmacy Statement"},
    "claimed_amount":     {"type":"number"},
    "approved_amount":    {"type":"number","description":"Final amount approved by the insurer. null if pending or under query"},
    "insurer_reference":  {"type":"string"},
    "approval_status":    {"type":"string","description":"Exactly one of: APPROVED, QUERY_RAISED, REJECTED"},
    "line_items": {"type":"array","items":{"type":"object","properties":{
        "description":{"type":"string"},"qty":{"type":"integer"},"rate":{"type":"number"},"amount":{"type":"number"}}}},
    "lab_results": {"type":"array","items":{"type":"object","properties":{
        "test_name":{"type":"string"},"value":{"type":"number"},"unit":{"type":"string"},"reference_range":{"type":"string"}}}},
    "trend_chart_values": {"type":"array","description":"Bar values shown in the trend chart image on the lab report page, left to right",
        "items":{"type":"number"}}
  }',
  map('version', '2.1', 'enableConfidenceScores', 'true', 'enableCitations', 'true')) AS ex
FROM bronze_parsed;

-- 4a. LOOK at one raw result first. Confirm the shape of scalar fields ({"value":..,"confidence_score":..})
--     and of ARRAY fields before trusting the flattening queries below.
SELECT file_name, ex:response, ex:error_message FROM silver_extract;

-- -------------------------------------------------------------------------------------
-- STEP 5  FLATTEN  (scalars: .value ; arrays: tolerant to both shapes)
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver_documents AS
SELECT file_name,
  ex:response.hospital_name.value::string        AS hospital_name,
  ex:response.hospital_gstin.value::string       AS hospital_gstin,
  ex:response.final_bill_no.value::string        AS final_bill_no,
  ex:response.ip_no.value::string                AS ip_no,
  try_cast(ex:response.bill_date.value::string AS DATE)        AS bill_date,
  try_cast(ex:response.payment_due_date.value::string AS DATE) AS payment_due_date,
  ex:response.patient_name.value::string         AS patient_name,
  nullif(trim(ex:response.patient_uhid.value::string), '')     AS patient_uhid,
  ex:response.age.value::int                     AS age,
  ex:response.gender.value::string               AS gender,
  try_cast(ex:response.admission_date.value::string AS DATE)   AS admission_date,
  try_cast(ex:response.discharge_date.value::string AS DATE)   AS discharge_date,
  ex:response.primary_diagnosis.value::string    AS primary_diagnosis,
  ex:response.procedure_performed.value::string  AS procedure_performed,
  ex:response.insurer_name.value::string         AS insurer_name,
  ex:response.policy_number.value::string        AS policy_number,
  ex:response.claim_type.value::string           AS claim_type,
  ex:response.subtotal.value::decimal(12,2)      AS subtotal,
  ex:response.discount.value::decimal(12,2)      AS discount,
  ex:response.gst.value::decimal(12,2)           AS gst,
  ex:response.grand_total_printed.value::decimal(12,2) AS grand_total_printed,
  ex:response.advance_paid.value::decimal(12,2)  AS advance_paid,
  ex:response.balance_payable.value::decimal(12,2) AS balance_payable,
  ex:response.lab_total.value::decimal(12,2)     AS lab_total,
  ex:response.pharmacy_total.value::decimal(12,2) AS pharmacy_total,
  ex:response.claimed_amount.value::decimal(12,2) AS claimed_amount,
  ex:response.approved_amount.value::decimal(12,2) AS approved_amount,
  ex:response.insurer_reference.value::string    AS insurer_reference,
  ex:response.approval_status.value::string      AS approval_status,
  ex:response.grand_total_printed.confidence_score::double AS grand_total_conf,
  ex:response.approved_amount.confidence_score::double     AS approved_amount_conf
FROM silver_extract;

-- Arrays: the array may sit at  field  or  field.value ; each item's attributes may be plain
-- or wrapped as {"value":..}. coalesce() + try_variant_get() handles both shapes.
CREATE OR REPLACE TABLE silver_line_items AS
SELECT file_name, li.pos + 1 AS line_no,
  coalesce(try_variant_get(li.value, '$.description.value', 'string'), try_variant_get(li.value, '$.description', 'string')) AS description,
  coalesce(try_variant_get(li.value, '$.qty.value', 'int'),            try_variant_get(li.value, '$.qty', 'int'))            AS qty,
  coalesce(try_variant_get(li.value, '$.rate.value', 'decimal(12,2)'), try_variant_get(li.value, '$.rate', 'decimal(12,2)')) AS rate,
  coalesce(try_variant_get(li.value, '$.amount.value', 'decimal(12,2)'), try_variant_get(li.value, '$.amount', 'decimal(12,2)')) AS amount
FROM silver_extract,
     LATERAL variant_explode(coalesce(ex:response.line_items.value, ex:response.line_items)) AS li;

CREATE OR REPLACE TABLE silver_lab_results AS
SELECT file_name,
  coalesce(try_variant_get(r.value, '$.test_name.value', 'string'), try_variant_get(r.value, '$.test_name', 'string')) AS test_name,
  coalesce(try_variant_get(r.value, '$.value.value', 'double'),     try_variant_get(r.value, '$.value', 'double'))     AS value,
  coalesce(try_variant_get(r.value, '$.unit.value', 'string'),      try_variant_get(r.value, '$.unit', 'string'))      AS unit,
  coalesce(try_variant_get(r.value, '$.reference_range.value', 'string'), try_variant_get(r.value, '$.reference_range', 'string')) AS reference_range
FROM silver_extract,
     LATERAL variant_explode(coalesce(ex:response.lab_results.value, ex:response.lab_results)) AS r;

CREATE OR REPLACE TABLE silver_chart_values AS
SELECT file_name, v.pos + 1 AS position,
       coalesce(try_variant_get(v.value, '$.value', 'double'), try_variant_get(v.value, '$', 'double')) AS value
FROM silver_extract,
     LATERAL variant_explode(coalesce(ex:response.trend_chart_values.value, ex:response.trend_chart_values)) AS v;

-- -------------------------------------------------------------------------------------
-- STEP 6  EVALUATE HEADER FIELDS  (field-level accuracy vs gt_documents)
-- <=> is null-safe equality: a correct NULL (e.g. UHID blank in packet C) counts as a match.
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE eval_field_scores AS
SELECT field, avg(ok) AS accuracy, sum(1 - ok) AS misses,
       collect_list(CASE WHEN ok = 0 THEN concat(file_name, ' -> got [', coalesce(got, 'NULL'), '] expected [', coalesce(exp, 'NULL'), ']') END) AS details
FROM (
  SELECT file_name, field, CASE WHEN ok THEN 1 ELSE 0 END AS ok, got, exp FROM (
    SELECT p.file_name,
      stack(23,
        'final_bill_no',      upper(p.final_bill_no) <=> upper(g.final_bill_no),          p.final_bill_no, g.final_bill_no,
        'ip_no',              upper(p.ip_no) <=> upper(g.ip_no),                          p.ip_no, g.ip_no,
        'bill_date',          p.bill_date <=> g.bill_date,                                string(p.bill_date), string(g.bill_date),
        'payment_due_date',   p.payment_due_date <=> g.payment_due_date,                  string(p.payment_due_date), string(g.payment_due_date),
        'patient_name',       upper(trim(p.patient_name)) <=> upper(g.patient_name),      p.patient_name, g.patient_name,
        'patient_uhid',       upper(p.patient_uhid) <=> upper(g.patient_uhid),            p.patient_uhid, g.patient_uhid,
        'age',                p.age <=> g.age,                                            string(p.age), string(g.age),
        'admission_date',     p.admission_date <=> g.admission_date,                      string(p.admission_date), string(g.admission_date),
        'discharge_date',     p.discharge_date <=> g.discharge_date,                      string(p.discharge_date), string(g.discharge_date),
        'policy_number',      upper(p.policy_number) <=> upper(g.policy_number),          p.policy_number, g.policy_number,
        'claim_type',         upper(p.claim_type) <=> upper(g.claim_type),                p.claim_type, g.claim_type,
        'subtotal',           p.subtotal <=> g.subtotal,                                  string(p.subtotal), string(g.subtotal),
        'discount',           p.discount <=> g.discount,                                  string(p.discount), string(g.discount),
        'gst',                p.gst <=> g.gst,                                            string(p.gst), string(g.gst),
        'grand_total_printed',p.grand_total_printed <=> g.grand_total_printed,            string(p.grand_total_printed), string(g.grand_total_printed),
        'advance_paid',       p.advance_paid <=> g.advance_paid,                          string(p.advance_paid), string(g.advance_paid),
        'balance_payable',    p.balance_payable <=> g.balance_payable,                    string(p.balance_payable), string(g.balance_payable),
        'lab_total',          p.lab_total <=> g.lab_total,                                string(p.lab_total), string(g.lab_total),
        'pharmacy_total',     p.pharmacy_total <=> g.pharmacy_total,                      string(p.pharmacy_total), string(g.pharmacy_total),
        'claimed_amount',     p.claimed_amount <=> g.claimed_amount,                      string(p.claimed_amount), string(g.claimed_amount),
        'approved_amount',    p.approved_amount <=> g.approved_amount,                    string(p.approved_amount), string(g.approved_amount),
        'insurer_reference',  upper(p.insurer_reference) <=> upper(g.insurer_reference),  p.insurer_reference, g.insurer_reference,
        'approval_status',    upper(p.approval_status) <=> upper(g.approval_status),      p.approval_status, g.approval_status
      ) AS (field, ok, got, exp)
    FROM silver_documents p JOIN gt_documents g USING (file_name)))
GROUP BY field;

SELECT * FROM eval_field_scores ORDER BY accuracy, field;
SELECT round(avg(accuracy), 3) AS overall_field_accuracy FROM eval_field_scores;

-- -------------------------------------------------------------------------------------
-- STEP 7  EVALUATE TABLES  (line items, lab results, chart values)
-- -------------------------------------------------------------------------------------
-- 7a. Line items: recall = share of true lines found with the same amount
SELECT g.file_name,
       count(*)                                                AS true_lines,
       count(p.amount)                                         AS matched_by_amount,
       round(count(p.amount) / count(*), 3)                    AS line_recall,
       (SELECT count(*) FROM silver_line_items x WHERE x.file_name = g.file_name) AS extracted_lines
FROM gt_line_items g
LEFT JOIN silver_line_items p
  ON p.file_name = g.file_name AND abs(p.amount - g.amount) < 0.01
GROUP BY g.file_name;

-- 7b. Lab results: value accuracy, matched on test name
SELECT g.file_name, g.test_name, g.value AS true_value, p.value AS extracted_value,
       CASE WHEN p.test_name IS NULL THEN 'NOT_FOUND'
            WHEN abs(p.value - g.value) < 0.001 THEN 'OK' ELSE 'WRONG_VALUE' END AS result
FROM gt_lab_results g
LEFT JOIN silver_lab_results p
  ON p.file_name = g.file_name AND lower(trim(p.test_name)) = lower(g.test_name)
ORDER BY result DESC, g.file_name;

-- 7c. Chart values (only exist inside the image): the hardest test
SELECT g.file_name, g.position, g.x_label, g.value AS true_value, p.value AS extracted_value,
       CASE WHEN abs(p.value - g.value) < 0.001 THEN 'OK' WHEN p.value IS NULL THEN 'MISSING' ELSE 'WRONG' END AS result
FROM gt_chart_values g
LEFT JOIN silver_chart_values p USING (file_name, position)
ORDER BY g.file_name, g.position;

-- -------------------------------------------------------------------------------------
-- STEP 8  VALIDATION  (the dependent-data rules) and compare with gt_validation
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver_validation AS
WITH li AS (
  SELECT file_name,
         sum(amount) AS items_sum,
         sum(CASE WHEN description ILIKE '%ICU%'  THEN qty ELSE 0 END) AS icu_days,
         sum(CASE WHEN description ILIKE '%Ward%' THEN qty ELSE 0 END) AS ward_days,
         max(CASE WHEN description ILIKE '%Laborator%' THEN amount END) AS bill_lab_line,
         max(CASE WHEN description ILIKE '%Pharmacy%'  THEN amount END) AS bill_pharmacy_line
  FROM silver_line_items GROUP BY file_name)
SELECT d.file_name, rule, CASE WHEN ok THEN 'PASS' ELSE 'FAIL' END AS actual
FROM silver_documents d LEFT JOIN li USING (file_name)
LATERAL VIEW stack(9,
  'GRAND_TOTAL_EQUALS_SUBTOTAL_MINUS_DISCOUNT_PLUS_GST', coalesce(abs(d.subtotal - d.discount + d.gst - d.grand_total_printed) < 1, false),
  'LINE_ITEMS_SUM_TO_SUBTOTAL',                          coalesce(abs(li.items_sum - d.subtotal) < 1, false),
  'WARD_PLUS_ICU_DAYS_EQUAL_LOS',                        coalesce(li.icu_days + li.ward_days = datediff(d.discharge_date, d.admission_date), false),
  'BILL_LAB_LINE_EQUALS_LAB_REPORT_TOTAL',               coalesce(abs(li.bill_lab_line - d.lab_total) < 0.01, false),
  'BILL_PHARMACY_LINE_EQUALS_PHARMACY_TOTAL',            coalesce(abs(li.bill_pharmacy_line - d.pharmacy_total) < 0.01, false),
  'CLAIMED_EQUALS_GRAND_TOTAL',                          coalesce(abs(d.claimed_amount - d.grand_total_printed) < 0.01, false),
  'PATIENT_UHID_PRESENT',                                d.patient_uhid IS NOT NULL,
  'INSURER_APPROVED',                                    coalesce(upper(d.approval_status) = 'APPROVED' AND d.approved_amount IS NOT NULL, false),
  'CONSULTANT_SIGNATURE_PRESENT',                        true   -- placeholder: see README (needs signature/figure check per page)
) AS rule, ok;

-- Did our validation reach the same verdicts as the answer key?
SELECT v.file_name, v.rule, v.actual, g.expected,
       CASE WHEN v.actual = g.expected THEN 'agree' ELSE '** DISAGREE **' END AS check
FROM silver_validation v JOIN gt_validation g USING (file_name, rule)
ORDER BY check DESC, v.file_name, v.rule;
-- Expected: packet A and B pass everything; packet C fails GRAND_TOTAL..., PATIENT_UHID_PRESENT,
-- INSURER_APPROVED, CONSULTANT_SIGNATURE_PRESENT. The signature rule uses a placeholder above,
-- so it will DISAGREE for C until you implement it, which makes a good exercise for Genie Code.

-- -------------------------------------------------------------------------------------
-- STEP 9  PUBLISH + REVIEW QUEUE
-- -------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE review_queue AS
SELECT file_name, collect_list(rule) AS failed_rules, current_timestamp() AS queued_at, 'OPEN' AS status
FROM silver_validation WHERE actual = 'FAIL' GROUP BY file_name;

CREATE OR REPLACE TABLE gold_claims_ready AS
SELECT d.* FROM silver_documents d
WHERE d.file_name NOT IN (SELECT file_name FROM review_queue);

SELECT 'gold' AS layer, count(*) FROM gold_claims_ready
UNION ALL SELECT 'review', count(*) FROM review_queue;      -- expect gold 2 (A, B), review 1 (C)
