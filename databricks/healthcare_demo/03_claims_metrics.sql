-- Healthcare Claims Metric View (v2 pattern)
-- Features: YAML joins, composed measures, agent metadata (display_name, format, synonyms)
-- No BI Compatibility Mode — PBI consumes via wrapper views (04_wrapper_views.sql)
-- Single source of truth for: Genie, AI/BI Dashboards, SQL, notebooks, PBI (via wrappers)

CREATE OR REPLACE VIEW vjoseph_pbi_demo.healthcare_demo.claims_metrics
WITH METRICS
LANGUAGE YAML
AS $yaml$
version: 1.1
comment: |-
  Healthcare claims KPIs with full star-schema joins.
  Source: fact_claims joined with dim_patients, dim_providers, dim_procedures, dim_facilities, dim_dates.
  Governed semantic layer — single source of truth for Genie, AI/BI, PBI wrapper views.
  Owner: VJ - Field Engineering
  Last updated: 2026-04-17

source: vjoseph_pbi_demo.healthcare_demo.fact_claims
joins:
  - name: patient
    source: vjoseph_pbi_demo.healthcare_demo.dim_patients
    using:
      - patient_id
  - name: provider
    source: vjoseph_pbi_demo.healthcare_demo.dim_providers
    using:
      - provider_id
  - name: proc
    source: vjoseph_pbi_demo.healthcare_demo.dim_procedures
    using:
      - procedure_id
  - name: facility
    source: vjoseph_pbi_demo.healthcare_demo.dim_facilities
    using:
      - facility_id
  - name: dt
    source: vjoseph_pbi_demo.healthcare_demo.dim_dates
    'on': claim_date = dt.date_key

dimensions:
  # --- Time dimensions (multi-granularity) ---
  - name: claim_date
    expr: claim_date
    display_name: Claim Date
  - name: claim_month
    expr: "DATE_TRUNC('MONTH', claim_date)"
    display_name: Claim Month
  - name: claim_year
    expr: YEAR(claim_date)
    display_name: Claim Year
  - name: quarter
    expr: dt.quarter
    display_name: Quarter
  - name: quarter_label
    expr: "CONCAT('Q', dt.quarter, ' ', YEAR(claim_date))"
    display_name: Quarter Label
  - name: fiscal_year
    expr: dt.fiscal_year
    display_name: Fiscal Year
  - name: fiscal_quarter
    expr: dt.fiscal_quarter
    display_name: Fiscal Quarter
  - name: month_name
    expr: dt.month_name
    display_name: Month Name

  # --- Clinical dimensions ---
  - name: procedure_name
    expr: proc.procedure_name
    display_name: Procedure
    synonyms:
      - procedure
      - service
  - name: procedure_category
    expr: proc.procedure_category
    display_name: Procedure Category
    synonyms:
      - category
      - service line
  - name: cpt_code
    expr: proc.cpt_code
    display_name: CPT Code
  - name: is_surgical
    expr: proc.is_surgical
    display_name: Is Surgical
  - name: primary_diagnosis_code
    expr: primary_diagnosis_code
    display_name: Primary Diagnosis (ICD-10)
    synonyms:
      - diagnosis
      - ICD code

  # --- Provider dimensions ---
  - name: provider_name
    expr: provider.provider_name
    display_name: Provider
    synonyms:
      - doctor
      - physician
  - name: specialty
    expr: provider.specialty
    display_name: Specialty
    synonyms:
      - department
  - name: provider_title
    expr: provider.title
    display_name: Provider Title

  # --- Facility dimensions ---
  - name: facility_name
    expr: facility.facility_name
    display_name: Facility
    synonyms:
      - hospital
      - clinic
      - location
  - name: facility_type
    expr: facility.facility_type
    display_name: Facility Type
  - name: facility_region
    expr: facility.region
    display_name: Region
    synonyms:
      - geography
      - area
  - name: facility_state
    expr: facility.state
    display_name: State
  - name: facility_city
    expr: facility.city
    display_name: City

  # --- Patient dimensions (non-PHI only) ---
  - name: gender
    expr: patient.gender
    display_name: Gender
  - name: ethnicity
    expr: patient.ethnicity
    display_name: Ethnicity
  - name: insurance_type
    expr: patient.insurance_type
    display_name: Insurance Type
    synonyms:
      - payer
      - coverage
  - name: plan_name
    expr: patient.plan_name
    display_name: Plan Name
    synonyms:
      - insurance plan
  - name: age_group
    expr: |-
      CASE
        WHEN FLOOR(DATEDIFF(claim_date, patient.dob) / 365.25) < 18 THEN 'Pediatric (<18)'
        WHEN FLOOR(DATEDIFF(claim_date, patient.dob) / 365.25) < 40 THEN 'Young Adult (18-39)'
        WHEN FLOOR(DATEDIFF(claim_date, patient.dob) / 365.25) < 65 THEN 'Adult (40-64)'
        ELSE 'Senior (65+)'
      END
    display_name: Age Group
    synonyms:
      - age bracket
      - age range

  # --- Claim attributes ---
  - name: claim_status
    expr: claim_status
    display_name: Claim Status
    synonyms:
      - status
  - name: is_readmission
    expr: is_readmission
    display_name: Is Readmission

measures:
  # --- Atomic measures (building blocks) ---
  - name: total_billed
    expr: SUM(billed_amount)
    display_name: Total Billed
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - billed
      - charges

  - name: total_allowed
    expr: SUM(allowed_amount)
    display_name: Total Allowed
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: total_paid
    expr: SUM(paid_amount)
    display_name: Total Paid
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - paid
      - reimbursement

  - name: total_patient_responsibility
    expr: SUM(patient_responsibility)
    display_name: Total Patient Responsibility
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - patient cost
      - out of pocket

  - name: claim_count
    expr: COUNT(DISTINCT claim_id)
    display_name: Claim Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact
    synonyms:
      - claims
      - number of claims

  - name: unique_patients
    expr: COUNT(DISTINCT patient_id)
    display_name: Unique Patients
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact
    synonyms:
      - patients

  - name: unique_providers
    expr: COUNT(DISTINCT provider_id)
    display_name: Unique Providers
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact

  - name: total_los_days
    expr: SUM(length_of_stay_days)
    display_name: Total Length of Stay (Days)
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  # --- Filtered measures ---
  - name: denied_claims
    expr: COUNT(DISTINCT claim_id) FILTER (WHERE claim_status = 'Denied')
    display_name: Denied Claims
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - denials

  - name: readmission_count
    expr: COUNT(DISTINCT claim_id) FILTER (WHERE is_readmission = TRUE)
    display_name: Readmissions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  - name: inpatient_claims
    expr: COUNT(DISTINCT claim_id) FILTER (WHERE length_of_stay_days > 0)
    display_name: Inpatient Claims
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  - name: surgical_revenue
    expr: SUM(billed_amount) FILTER (WHERE proc.is_surgical = TRUE)
    display_name: Surgical Revenue
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: emergency_claims
    expr: COUNT(DISTINCT claim_id) FILTER (WHERE proc.procedure_category = 'Emergency')
    display_name: Emergency Claims
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  # --- Composed measures (reference atomic/filtered measures via MEASURE()) ---
  - name: avg_claim_value
    expr: MEASURE(total_billed) / NULLIF(MEASURE(claim_count), 0)
    display_name: Avg Claim Value
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - average claim

  - name: payment_rate
    expr: MEASURE(total_paid) / NULLIF(MEASURE(total_billed), 0)
    display_name: Payment Rate
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
    synonyms:
      - collection rate
      - reimbursement rate

  - name: denial_rate
    expr: MEASURE(denied_claims) / NULLIF(MEASURE(claim_count), 0)
    display_name: Denial Rate
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1

  - name: avg_patient_responsibility
    expr: MEASURE(total_patient_responsibility) / NULLIF(MEASURE(claim_count), 0)
    display_name: Avg Patient Responsibility
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2

  - name: avg_length_of_stay
    expr: MEASURE(total_los_days) / NULLIF(MEASURE(inpatient_claims), 0)
    display_name: Avg Length of Stay
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
    synonyms:
      - ALOS

  - name: readmission_rate
    expr: MEASURE(readmission_count) / NULLIF(MEASURE(claim_count), 0)
    display_name: Readmission Rate
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1

  - name: revenue_per_patient
    expr: MEASURE(total_billed) / NULLIF(MEASURE(unique_patients), 0)
    display_name: Revenue per Patient
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
$yaml$;
