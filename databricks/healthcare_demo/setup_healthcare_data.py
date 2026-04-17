"""
Generate SQL statements for the healthcare demo star schema.
Runs via Databricks Statement Execution API.
Schema: vjoseph_pbi_demo.healthcare_demo
"""
import json
import subprocess
import sys
import time

PROFILE = "adb-984752964297111"
WAREHOUSE_ID = "54e77213da593af3"
CATALOG = "vjoseph_pbi_demo"
SCHEMA = "healthcare_demo"
FQN = f"{CATALOG}.{SCHEMA}"


def run_sql(statement, label="", timeout="50s"):
    """Execute SQL via Statement Execution API and return result."""
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": timeout
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         f"--profile={PROFILE}", "--json", payload],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ERROR ({label}): {result.stderr.strip()}")
        return None
    try:
        r = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  ERROR ({label}): Could not parse response")
        return None

    state = r.get("status", {}).get("state", "UNKNOWN")
    if state == "SUCCEEDED":
        print(f"  OK: {label}")
        return r
    elif state == "PENDING":
        # Poll for completion
        stmt_id = r.get("statement_id")
        for _ in range(30):
            time.sleep(2)
            poll = subprocess.run(
                ["databricks", "api", "get",
                 f"/api/2.0/sql/statements/{stmt_id}",
                 f"--profile={PROFILE}"],
                capture_output=True, text=True
            )
            pr = json.loads(poll.stdout)
            ps = pr.get("status", {}).get("state")
            if ps == "SUCCEEDED":
                print(f"  OK: {label}")
                return pr
            elif ps in ("FAILED", "CANCELED", "CLOSED"):
                err = pr.get("status", {}).get("error", {}).get("message", ps)
                print(f"  FAILED ({label}): {err}")
                return None
        print(f"  TIMEOUT ({label})")
        return None
    else:
        err = r.get("status", {}).get("error", {}).get("message", state)
        print(f"  FAILED ({label}): {err}")
        return None


# ============================================================
# dim_dates
# ============================================================
def create_dim_dates():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.dim_dates AS
    WITH date_range AS (
      SELECT explode(sequence(DATE'2022-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS date_key
    )
    SELECT
      date_key,
      YEAR(date_key) AS year,
      QUARTER(date_key) AS quarter,
      MONTH(date_key) AS month_num,
      DATE_FORMAT(date_key, 'MMMM') AS month_name,
      DATE_FORMAT(date_key, 'MMM') AS month_short,
      DAY(date_key) AS day_of_month,
      DAYOFWEEK(date_key) AS day_of_week,
      DATE_FORMAT(date_key, 'EEEE') AS day_name,
      WEEKOFYEAR(date_key) AS week_of_year,
      CONCAT('FY', CASE WHEN MONTH(date_key) >= 10 THEN YEAR(date_key)+1 ELSE YEAR(date_key) END) AS fiscal_year,
      CONCAT('FQ', CASE
        WHEN MONTH(date_key) IN (10,11,12) THEN 1
        WHEN MONTH(date_key) IN (1,2,3) THEN 2
        WHEN MONTH(date_key) IN (4,5,6) THEN 3
        ELSE 4
      END) AS fiscal_quarter,
      CASE WHEN DAYOFWEEK(date_key) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM date_range
    """
    return run_sql(sql, "dim_dates")


# ============================================================
# dim_facilities
# ============================================================
def create_dim_facilities():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.dim_facilities AS
    SELECT * FROM VALUES
      (1, 'Mercy General Hospital', 'Hospital', 'Sacramento', 'CA', 'West', 450),
      (2, 'St. Luke''s Medical Center', 'Hospital', 'Phoenix', 'AZ', 'West', 380),
      (3, 'Downtown Urgent Care', 'Urgent Care', 'Portland', 'OR', 'West', 0),
      (4, 'Northside Family Clinic', 'Clinic', 'Atlanta', 'GA', 'Southeast', 0),
      (5, 'Memorial Regional Hospital', 'Hospital', 'Hollywood', 'FL', 'Southeast', 520),
      (6, 'Lakeview Emergency Center', 'Emergency', 'Chicago', 'IL', 'Midwest', 0),
      (7, 'Summit Surgical Center', 'Surgical Center', 'Denver', 'CO', 'West', 0),
      (8, 'Bayview Community Hospital', 'Hospital', 'Tampa', 'FL', 'Southeast', 290),
      (9, 'Riverside Pediatric Clinic', 'Clinic', 'Austin', 'TX', 'South', 0),
      (10, 'Heritage Medical Center', 'Hospital', 'Boston', 'MA', 'Northeast', 410),
      (11, 'Valley Women''s Health', 'Clinic', 'San Jose', 'CA', 'West', 0),
      (12, 'Heartland Regional Hospital', 'Hospital', 'Kansas City', 'MO', 'Midwest', 340),
      (13, 'Pacific Coast Emergency', 'Emergency', 'San Diego', 'CA', 'West', 0),
      (14, 'Mountain View Rehab Center', 'Rehab', 'Salt Lake City', 'UT', 'West', 120),
      (15, 'Capital Cardiology Clinic', 'Clinic', 'Washington', 'DC', 'Northeast', 0)
    AS t(facility_id, facility_name, facility_type, city, state, region, bed_count)
    """
    return run_sql(sql, "dim_facilities")


# ============================================================
# dim_procedures
# ============================================================
def create_dim_procedures():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.dim_procedures AS
    SELECT * FROM VALUES
      (1, '99213', 'Office Visit - Established Patient', 'Office Visit', 15, FALSE),
      (2, '99214', 'Office Visit - Detailed', 'Office Visit', 25, FALSE),
      (3, '99203', 'Office Visit - New Patient', 'Office Visit', 30, FALSE),
      (4, '99283', 'Emergency Visit - Moderate', 'Emergency', 45, FALSE),
      (5, '99284', 'Emergency Visit - High Severity', 'Emergency', 90, FALSE),
      (6, '99285', 'Emergency Visit - Critical', 'Emergency', 120, FALSE),
      (7, '27447', 'Total Knee Replacement', 'Orthopedic Surgery', 180, TRUE),
      (8, '27130', 'Total Hip Replacement', 'Orthopedic Surgery', 150, TRUE),
      (9, '33533', 'Coronary Artery Bypass', 'Cardiac Surgery', 300, TRUE),
      (10, '47562', 'Laparoscopic Cholecystectomy', 'General Surgery', 90, TRUE),
      (11, '43239', 'Upper GI Endoscopy with Biopsy', 'Gastroenterology', 30, FALSE),
      (12, '71046', 'Chest X-Ray (2 views)', 'Radiology', 10, FALSE),
      (13, '70553', 'Brain MRI with Contrast', 'Radiology', 45, FALSE),
      (14, '93000', 'Electrocardiogram (ECG)', 'Cardiology', 15, FALSE),
      (15, '93306', 'Echocardiogram', 'Cardiology', 45, FALSE),
      (16, '90834', 'Psychotherapy 45 min', 'Behavioral Health', 45, FALSE),
      (17, '90837', 'Psychotherapy 60 min', 'Behavioral Health', 60, FALSE),
      (18, '36415', 'Blood Draw (Venipuncture)', 'Lab', 5, FALSE),
      (19, '80053', 'Comprehensive Metabolic Panel', 'Lab', 5, FALSE),
      (20, '85025', 'Complete Blood Count (CBC)', 'Lab', 5, FALSE),
      (21, '29881', 'Knee Arthroscopy', 'Orthopedic Surgery', 60, TRUE),
      (22, '49505', 'Inguinal Hernia Repair', 'General Surgery', 75, TRUE),
      (23, '59400', 'Obstetric Delivery', 'Obstetrics', 240, TRUE),
      (24, '99232', 'Inpatient Visit - Moderate', 'Inpatient', 30, FALSE),
      (25, '99233', 'Inpatient Visit - High', 'Inpatient', 45, FALSE)
    AS t(procedure_id, cpt_code, procedure_name, procedure_category, avg_duration_mins, is_surgical)
    """
    return run_sql(sql, "dim_procedures")


# ============================================================
# dim_providers
# ============================================================
def create_dim_providers():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.dim_providers AS
    SELECT * FROM VALUES
      (1, 'Dr. Sarah Chen', 'Cardiology', '1234567890', 'Attending'),
      (2, 'Dr. James Wilson', 'Orthopedic Surgery', '2345678901', 'Attending'),
      (3, 'Dr. Maria Rodriguez', 'Family Medicine', '3456789012', 'Attending'),
      (4, 'Dr. David Kim', 'Emergency Medicine', '4567890123', 'Attending'),
      (5, 'Dr. Emily Johnson', 'General Surgery', '5678901234', 'Attending'),
      (6, 'Dr. Robert Patel', 'Gastroenterology', '6789012345', 'Attending'),
      (7, 'Dr. Lisa Thompson', 'Radiology', '7890123456', 'Attending'),
      (8, 'Dr. Michael Brown', 'Behavioral Health', '8901234567', 'Attending'),
      (9, 'Dr. Jennifer Davis', 'Obstetrics', '9012345678', 'Attending'),
      (10, 'Dr. William Garcia', 'Pediatrics', '0123456789', 'Attending'),
      (11, 'NP. Amanda White', 'Family Medicine', '1111111111', 'Nurse Practitioner'),
      (12, 'PA. Kevin Lee', 'Orthopedic Surgery', '2222222222', 'Physician Assistant'),
      (13, 'Dr. Rachel Scott', 'Internal Medicine', '3333333333', 'Attending'),
      (14, 'Dr. Thomas Martinez', 'Cardiology', '4444444444', 'Attending'),
      (15, 'Dr. Jessica Taylor', 'Emergency Medicine', '5555555555', 'Attending')
    AS t(provider_id, provider_name, specialty, npi_number, title)
    """
    return run_sql(sql, "dim_providers")


# ============================================================
# dim_patients (WITH PHI: SSN, DOB, patient_name)
# ============================================================
def create_dim_patients():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.dim_patients AS
    WITH patient_base AS (
      SELECT
        id AS patient_id,
        CASE (id % 20)
          WHEN 0 THEN 'John Smith' WHEN 1 THEN 'Maria Garcia' WHEN 2 THEN 'James Johnson'
          WHEN 3 THEN 'Patricia Williams' WHEN 4 THEN 'Robert Brown' WHEN 5 THEN 'Linda Jones'
          WHEN 6 THEN 'Michael Davis' WHEN 7 THEN 'Barbara Miller' WHEN 8 THEN 'William Wilson'
          WHEN 9 THEN 'Elizabeth Moore' WHEN 10 THEN 'David Taylor' WHEN 11 THEN 'Jennifer Anderson'
          WHEN 12 THEN 'Richard Thomas' WHEN 13 THEN 'Susan Jackson' WHEN 14 THEN 'Joseph White'
          WHEN 15 THEN 'Margaret Harris' WHEN 16 THEN 'Charles Martin' WHEN 17 THEN 'Dorothy Thompson'
          WHEN 18 THEN 'Daniel Robinson' ELSE 'Nancy Clark'
        END AS base_name,
        CASE (id % 6)
          WHEN 0 THEN 'Male' WHEN 1 THEN 'Female' WHEN 2 THEN 'Male'
          WHEN 3 THEN 'Female' WHEN 4 THEN 'Male' ELSE 'Female'
        END AS gender,
        CASE (id % 4)
          WHEN 0 THEN 'White' WHEN 1 THEN 'Hispanic' WHEN 2 THEN 'Black' ELSE 'Asian'
        END AS ethnicity,
        CASE (id % 5)
          WHEN 0 THEN 'Commercial' WHEN 1 THEN 'Medicare' WHEN 2 THEN 'Medicaid'
          WHEN 3 THEN 'Commercial' ELSE 'Self-Pay'
        END AS insurance_type,
        CASE (id % 8)
          WHEN 0 THEN 'Blue Cross PPO' WHEN 1 THEN 'Medicare Part A' WHEN 2 THEN 'Medicaid Standard'
          WHEN 3 THEN 'Aetna HMO' WHEN 4 THEN 'Self-Pay' WHEN 5 THEN 'United Healthcare'
          WHEN 6 THEN 'Cigna PPO' ELSE 'Medicare Advantage'
        END AS plan_name,
        CASE (id % 8)
          WHEN 0 THEN 'CA' WHEN 1 THEN 'FL' WHEN 2 THEN 'TX' WHEN 3 THEN 'IL'
          WHEN 4 THEN 'AZ' WHEN 5 THEN 'GA' WHEN 6 THEN 'MA' ELSE 'OR'
        END AS state,
        CASE (id % 8)
          WHEN 0 THEN '90210' WHEN 1 THEN '33101' WHEN 2 THEN '73301' WHEN 3 THEN '60601'
          WHEN 4 THEN '85001' WHEN 5 THEN '30301' WHEN 6 THEN '02101' ELSE '97201'
        END AS zip_code
      FROM (SELECT explode(sequence(1, 5000)) AS id)
    )
    SELECT
      patient_id,
      CONCAT(base_name, ' ', CAST(patient_id AS STRING)) AS patient_name,
      -- PHI: Synthetic SSN
      CONCAT(
        LPAD(CAST((patient_id * 7 + 100) % 900 + 100 AS STRING), 3, '0'), '-',
        LPAD(CAST((patient_id * 13 + 10) % 90 + 10 AS STRING), 2, '0'), '-',
        LPAD(CAST((patient_id * 31 + 1000) % 9000 + 1000 AS STRING), 4, '0')
      ) AS ssn,
      -- PHI: Date of birth (ages 18-95)
      DATE_ADD(DATE'1930-01-01', (patient_id * 397) % 28000) AS dob,
      gender,
      ethnicity,
      insurance_type,
      plan_name,
      state,
      zip_code,
      DATE_ADD(DATE'2020-01-01', (patient_id * 53) % 1400) AS enrollment_date
    FROM patient_base
    """
    return run_sql(sql, "dim_patients")


# ============================================================
# fact_claims (~500K rows)
# ============================================================
def create_fact_claims():
    sql = f"""
    CREATE OR REPLACE TABLE {FQN}.fact_claims AS
    WITH claim_gen AS (
      SELECT
        id AS claim_id,
        -- Patient: 1-5000
        (id % 5000) + 1 AS patient_id,
        -- Provider: 1-15
        (id % 15) + 1 AS provider_id,
        -- Procedure: 1-25
        (id % 25) + 1 AS procedure_id,
        -- Facility: 1-15
        (id % 15) + 1 AS facility_id,
        -- Claim date: 2022-01-01 to 2025-12-31 (~4 years)
        DATE_ADD(DATE'2022-01-01', (id * 3) % 1461) AS claim_date,
        -- Status distribution: 70% Paid, 12% Denied, 10% Pending, 5% Under Review, 3% Appealed
        CASE
          WHEN (id * 7) % 100 < 70 THEN 'Paid'
          WHEN (id * 7) % 100 < 82 THEN 'Denied'
          WHEN (id * 7) % 100 < 92 THEN 'Pending'
          WHEN (id * 7) % 100 < 97 THEN 'Under Review'
          ELSE 'Appealed'
        END AS claim_status,
        -- Billed amount varies by procedure type (we'll adjust later)
        ROUND(50 + (id * 31 % 5000) * 0.5 + ((id * 7) % 100) * 10.0, 2) AS base_billed
      FROM (SELECT explode(sequence(1, 500000)) AS id)
    ),
    claim_amounts AS (
      SELECT
        c.*,
        -- Adjust billed amount by procedure category
        ROUND(
          CASE
            WHEN p.is_surgical THEN c.base_billed * 8.0 + 5000
            WHEN p.procedure_category = 'Emergency' THEN c.base_billed * 3.0 + 800
            WHEN p.procedure_category = 'Radiology' THEN c.base_billed * 1.5 + 200
            WHEN p.procedure_category = 'Lab' THEN c.base_billed * 0.2 + 20
            ELSE c.base_billed + 100
          END, 2
        ) AS billed_amount
      FROM claim_gen c
      JOIN {FQN}.dim_procedures p ON c.procedure_id = p.procedure_id
    )
    SELECT
      c.claim_id,
      c.patient_id,
      c.provider_id,
      c.procedure_id,
      c.facility_id,
      c.claim_date,
      c.claim_status,
      c.billed_amount,
      -- Allowed = 60-90% of billed
      ROUND(c.billed_amount * (0.6 + ((c.claim_id * 17) % 30) * 0.01), 2) AS allowed_amount,
      -- Paid = allowed * payment factor (0 if denied)
      ROUND(
        CASE
          WHEN c.claim_status = 'Denied' THEN 0
          WHEN c.claim_status = 'Pending' THEN 0
          WHEN c.claim_status = 'Under Review' THEN 0
          ELSE c.billed_amount * (0.6 + ((c.claim_id * 17) % 30) * 0.01) * (0.75 + ((c.claim_id * 23) % 25) * 0.01)
        END, 2
      ) AS paid_amount,
      -- Patient responsibility
      ROUND(
        CASE
          WHEN c.claim_status = 'Denied' THEN c.billed_amount
          ELSE c.billed_amount * (0.05 + ((c.claim_id * 11) % 15) * 0.01)
        END, 2
      ) AS patient_responsibility,
      -- Diagnosis code (ICD-10 style)
      CASE (c.claim_id % 15)
        WHEN 0 THEN 'I25.10' WHEN 1 THEN 'M17.11' WHEN 2 THEN 'E11.9'
        WHEN 3 THEN 'J18.9' WHEN 4 THEN 'K80.20' WHEN 5 THEN 'S72.001A'
        WHEN 6 THEN 'I10' WHEN 7 THEN 'F32.1' WHEN 8 THEN 'N18.3'
        WHEN 9 THEN 'G43.909' WHEN 10 THEN 'J06.9' WHEN 11 THEN 'M54.5'
        WHEN 12 THEN 'R10.9' WHEN 13 THEN 'Z23' ELSE 'Z00.00'
      END AS primary_diagnosis_code,
      -- Length of stay (only for inpatient/surgical)
      CASE
        WHEN p.is_surgical THEN 1 + (c.claim_id % 7)
        WHEN p.procedure_category = 'Inpatient' THEN 1 + (c.claim_id % 5)
        ELSE 0
      END AS length_of_stay_days,
      -- Readmission flag (5% of claims)
      CASE WHEN (c.claim_id * 41) % 100 < 5 THEN TRUE ELSE FALSE END AS is_readmission
    FROM claim_amounts c
    JOIN {FQN}.dim_procedures p ON c.procedure_id = p.procedure_id
    """
    return run_sql(sql, "fact_claims (500K rows — may take a moment)")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print(f"Healthcare Demo Setup — {FQN}")
    print("=" * 60)

    print("\n[1/5] Creating dim_dates...")
    create_dim_dates()

    print("\n[2/5] Creating dim_facilities...")
    create_dim_facilities()

    print("\n[3/5] Creating dim_procedures...")
    create_dim_procedures()

    print("\n[4/5] Creating dim_providers...")
    create_dim_providers()

    print("\n[5/5] Creating dim_patients (with PHI)...")
    create_dim_patients()

    print("\n[6/6] Creating fact_claims (500K rows)...")
    create_fact_claims()

    print("\n" + "=" * 60)
    print("Done! Verify with:")
    print(f"  SELECT COUNT(*) FROM {FQN}.fact_claims")
    print("=" * 60)
