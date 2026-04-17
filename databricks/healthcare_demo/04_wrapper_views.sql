-- PBI Wrapper Views
-- These views pre-apply MEASURE() from the metric view and expose regular columns.
-- PBI queries these in Direct Query mode — no BI Compatibility Mode needed.
-- Governance flows through: metric view → wrapper view → PBI

-- ============================================================
-- vw_claims_summary: Pre-aggregated KPIs for PBI dashboards
-- Reduces 500K raw rows to ~76K aggregated rows (85% reduction)
-- PBI gets fast Direct Query performance with governed measures
-- ============================================================
CREATE OR REPLACE VIEW vjoseph_pbi_demo.healthcare_demo.vw_claims_summary AS
SELECT
  claim_month,
  claim_year,
  quarter_label,
  fiscal_year,
  month_name,
  procedure_category,
  is_surgical,
  specialty,
  facility_name,
  facility_type,
  facility_region,
  facility_state,
  gender,
  ethnicity,
  insurance_type,
  plan_name,
  age_group,
  claim_status,
  MEASURE(`total_billed`) as total_billed,
  MEASURE(`total_allowed`) as total_allowed,
  MEASURE(`total_paid`) as total_paid,
  MEASURE(`total_patient_responsibility`) as total_patient_responsibility,
  MEASURE(`claim_count`) as claim_count,
  MEASURE(`unique_patients`) as unique_patients,
  MEASURE(`denied_claims`) as denied_claims,
  MEASURE(`readmission_count`) as readmission_count,
  MEASURE(`inpatient_claims`) as inpatient_claims,
  MEASURE(`surgical_revenue`) as surgical_revenue,
  MEASURE(`emergency_claims`) as emergency_claims,
  MEASURE(`total_los_days`) as total_los_days
FROM vjoseph_pbi_demo.healthcare_demo.claims_metrics
GROUP BY
  claim_month, claim_year, quarter_label, fiscal_year, month_name,
  procedure_category, is_surgical, specialty,
  facility_name, facility_type, facility_region, facility_state,
  gender, ethnicity, insurance_type, plan_name, age_group, claim_status;


-- ============================================================
-- vw_claims_detail: Row-level claim data with UC-masked PHI
-- Used for drill-through tables in PBI reports
-- Column masks on dim_patients (ssn, dob, patient_name) are
-- automatically enforced by UC based on the querying user's identity
-- ============================================================
CREATE OR REPLACE VIEW vjoseph_pbi_demo.healthcare_demo.vw_claims_detail AS
SELECT
  f.claim_id,
  f.claim_date,
  f.claim_status,
  f.billed_amount,
  f.allowed_amount,
  f.paid_amount,
  f.patient_responsibility,
  f.primary_diagnosis_code,
  f.length_of_stay_days,
  f.is_readmission,
  -- Patient info (column masks applied automatically by UC)
  p.patient_name,
  p.gender,
  p.insurance_type,
  p.plan_name,
  p.state AS patient_state,
  -- Provider
  pr.provider_name,
  pr.specialty,
  -- Procedure
  pc.procedure_name,
  pc.procedure_category,
  pc.cpt_code,
  pc.is_surgical,
  -- Facility
  fa.facility_name,
  fa.facility_type,
  fa.region AS facility_region
FROM vjoseph_pbi_demo.healthcare_demo.fact_claims f
JOIN vjoseph_pbi_demo.healthcare_demo.dim_patients p ON f.patient_id = p.patient_id
JOIN vjoseph_pbi_demo.healthcare_demo.dim_providers pr ON f.provider_id = pr.provider_id
JOIN vjoseph_pbi_demo.healthcare_demo.dim_procedures pc ON f.procedure_id = pc.procedure_id
JOIN vjoseph_pbi_demo.healthcare_demo.dim_facilities fa ON f.facility_id = fa.facility_id;
