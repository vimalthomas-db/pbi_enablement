-- Column masking functions for PHI protection
-- Applied to dim_patients: ssn, dob, patient_name
-- Admins group sees real data; everyone else sees masked values

-- SSN: shows only last 4 digits
CREATE OR REPLACE FUNCTION vjoseph_pbi_demo.healthcare_demo.mask_ssn(ssn_val STRING)
RETURN CASE
  WHEN is_account_group_member('admins') THEN ssn_val
  ELSE CONCAT('***-**-', RIGHT(ssn_val, 4))
END;

-- DOB: truncates to year only
CREATE OR REPLACE FUNCTION vjoseph_pbi_demo.healthcare_demo.mask_dob(dob_val DATE)
RETURN CASE
  WHEN is_account_group_member('admins') THEN dob_val
  ELSE DATE_TRUNC('YEAR', dob_val)
END;

-- Patient name: shows first initial + last name
CREATE OR REPLACE FUNCTION vjoseph_pbi_demo.healthcare_demo.mask_patient_name(name_val STRING)
RETURN CASE
  WHEN is_account_group_member('admins') THEN name_val
  ELSE CONCAT(LEFT(name_val, 1), '**** ', SPLIT(name_val, ' ')[1])
END;

-- Apply masks to dim_patients
ALTER TABLE vjoseph_pbi_demo.healthcare_demo.dim_patients
  ALTER COLUMN ssn SET MASK vjoseph_pbi_demo.healthcare_demo.mask_ssn;

ALTER TABLE vjoseph_pbi_demo.healthcare_demo.dim_patients
  ALTER COLUMN dob SET MASK vjoseph_pbi_demo.healthcare_demo.mask_dob;

ALTER TABLE vjoseph_pbi_demo.healthcare_demo.dim_patients
  ALTER COLUMN patient_name SET MASK vjoseph_pbi_demo.healthcare_demo.mask_patient_name;
