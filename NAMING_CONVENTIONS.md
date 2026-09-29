# Data Naming Standards

**Project:** Healthcare Data Engineering Portfolio — Silver Layer
**Version:** 1.1
**Last Updated:** September 2026

---

## Purpose

This document defines the naming conventions used across all Silver layer tables in the Databricks healthcare data pipeline. Consistent naming makes schemas self-documenting, reduces ambiguity, and improves collaboration between data engineers, analysts, and AI agents that consume this schema.

> **For Validation Only:** This document is structured as machine-readable tables. Each rule has an ID (e.g., `R1`) and can be validated programmatically. See the [Validation Checklist](#validation-checklist) at the end.

---

## Abbreviation Reference

| Full Term | Abbreviation | Example |
|---|---|---|
| name | `nm` | `full_nm`, `source_file_nm` |
| date | `dt` | `birth_dt`, `recorded_dt` |
| time / timestamp / datetime | `ts` | `ingest_ts`, `authored_ts` |
| number (value-identifier) | `num` | `phone_num`, `social_security_num` |
| code | `cd` | `race_cd`, `status_cd` |
| count | `ct` | `item_ct`, `participant_ct` |
| identifier (KEY only) | `id` | `patient_id`, `encounter_id` |
| amount | `amt` | `total_amt`, `payment_amt` |
| latitude | `lat` | `patient_lat` |
| longitude | `long` | `patient_long` |

---

## Naming Rules

### R1 — `name` → `nm`

Any column representing a name (person name, file name, organization name) uses `nm`.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `full_name` | `full_nm` | Full Name |
| `first_name` | `first_nm` | First Name |
| `last_name` | `last_nm` | Last Name |
| `practitioner_name` | `practitioner_nm` | Practitioner Name |
| `location_name` | `location_nm` | Location Name |
| `source_file` | `source_file_nm` | Source File Name |

---

### R2 — `date` → `dt`

Any column representing a calendar date (no time component) uses `dt`.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `birth_date` | `birth_dt` | Birth Date |
| `recorded_date` | `recorded_dt` | Recorded Date |

---

### R3 — `time` / `timestamp` / `datetime` → `ts`

Any column representing a point in time (with time component) uses `ts`. This includes fields originally named `datetime`.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `period_start` | `period_start_ts` | Period Start Timestamp |
| `period_end` | `period_end_ts` | Period End Timestamp |
| `authored_on` | `authored_ts` | Authored Timestamp |
| `created` | `created_ts` | Created Timestamp |
| `issued` | `issued_ts` | Issued Timestamp |
| `onset_datetime` | `onset_ts` | Onset Timestamp |
| `abatement_datetime` | `abatement_ts` | Abatement Timestamp |
| `effective_datetime` | `effective_ts` | Effective Timestamp |
| `occurrence_datetime` | `occurrence_ts` | Occurrence Timestamp |
| `ingested_at` | `ingest_ts` | Ingest Timestamp |
| `recorded` | `recorded_ts` | Recorded Timestamp |

---

### R4 — Key Identifiers → `id`

**Primary keys and foreign keys** use `_id` because they **reference another entity** in the data model.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `id` (PK) | `patient_id` | Patient Identifier |
| `id` (PK) | `encounter_id` | Encounter Identifier |
| `subject.reference` | `patient_id` | Patient Identifier (FK) |
| `encounter.reference` | `encounter_id` | Encounter Identifier (FK) |
| `claim.reference` | `claim_id` | Claim Identifier (FK) |

---

### R5 — Value Identifiers → `num`

**Identifiers that are VALUES** (not keys) keep `_num` because "number" describes the value itself. Abbreviated identifiers must first be expanded, then have the rule applied.

| Raw Column | Expanded Form | Standardized Column |
|---|---|---|
| `mrn` | `medical_record_number` | `medical_record_num` |
| `ssn` | `social_security_number` | `social_security_num` |
| `drivers_license` | `drivers_license_number` | `drivers_license_num` |
| `passport_number` | `passport_number` | `passport_num` |
| `phone` | `phone_number` | `phone_num` |

> **Distinction:** `medical_record_num` is a *value* stored on the patient. `patient_id` is a *key* that other tables use to reference the patient. Both exist and serve different purposes.

---

### R6 — Coded Values → `cd`

Any column representing a coded value from a controlled vocabulary or set list uses `cd`. This includes fields originally named `code`.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `race` | `race_cd` | Race Code |
| `ethnicity` | `ethnicity_cd` | Ethnicity Code |
| `marital_status` | `marital_status_cd` | Marital Status Code |
| `language` | `language_cd` | Language Code |
| `gender` | `gender_cd` | Gender Code |
| `status` | `status_cd` | Status Code |
| `class_code` | `class_cd` | Class Code |
| `clinical_status` | `clinical_status_cd` | Clinical Status Code |
| `verification_status` | `verification_status_cd` | Verification Status Code |
| `category` | `category_cd` | Category Code |
| `code` | `code_cd` | Code |
| `vaccine_code` | `vaccine_cd` | Vaccine Code |
| `reason_code` | `reason_cd` | Reason Code |

---

### R7 — Counts → `ct`

Any column representing a numeric count of items uses `ct`.

| Raw Column | Standardized Column | Logical Name |
|---|---|---|
| `item_count` | `item_ct` | Item Count |
| `participant_count` | `participant_ct` | Participant Count |
| `activity_count` | `activity_ct` | Activity Count |
| `result_count` | `result_ct` | Result Count |
| `component_count` | `component_ct` | Component Count |
| `target_count` | `target_ct` | Target Count |

---

### R8 — Resource Prefix for Ambiguous Columns

Columns that could belong to multiple resources, or that would be ambiguous without context, are prefixed with the resource type.

| Ambiguous Column | Standardized Column | Logical Name |
|---|---|---|
| `city` | `patient_city` | Patient City |
| `state` | `patient_state_cd` | Patient State Code |
| `postal_code` | `patient_postal_cd` | Patient Postal Code |
| `country` | `patient_country_cd` | Patient Country Code |
| `latitude` | `patient_lat` | Patient Latitude |
| `longitude` | `patient_long` | Patient Longitude |
| `address_line` | `patient_address_line` | Patient Address Line |

> Columns already unambiguous due to FK context (e.g., `provider_nm` in a claim table) do **not** require a prefix.

---

## Suffix Reference

| Suffix | Meaning | Example |
|---|---|---|
| `_id` | Key (PK or FK) | `patient_id`, `encounter_id` |
| `_num` | Value-identifier (numbers) | `medical_record_num`, `phone_num` |
| `_nm` | Name | `full_nm`, `source_file_nm` |
| `_dt` | Date (no time) | `birth_dt` |
| `_ts` | Timestamp | `ingest_ts` |
| `_cd` | Code | `race_cd` |
| `_ct` | Count | `item_ct` |
| `_amt` | Amount / monetary value | `total_amt` |
| `_lat` | Latitude | `patient_lat` |
| `_long` | Longitude | `patient_long` |
| `_line` | Address line | `patient_address_line` |
| `_val` | Generic value | `value_val` |
| `_text` | Free text | `medication_text` |
| `_desc` | Description | `reason_desc` |
| `_flag` | Boolean flag | `is_active_flag` |

---

## General Principles

1. Use **snake_case** for all column names.
2. Use **lowercase** for all column names.
3. Do **not** use spaces, hyphens, or special characters.
4. Primary keys follow `{resource}_id`.
5. Foreign keys follow `{referenced_resource}_id`.
6. Value-identifiers (SSN, phone, license, passport, MRN) use `_num`.
7. Boolean columns use an `is_` prefix where possible (`is_active_flag`, `is_primary_flag`).
8. PHI columns must be flagged in the data design document and masked via Unity Catalog dynamic data masking functions.

---

## Validation Checklist

> **For Validation Only:** To validate a new table against these standards, apply the checks below in order. Each check references a rule above.

| Check ID | Rule | Validation Logic |
|---|---|---|
| V1 | R1 | If raw column contains `name` → column must end in `_nm` |
| V2 | R2 | If raw column contains `date` (no time) → column must end in `_dt` |
| V3 | R3 | If raw column contains `time`, `timestamp`, or `datetime` → column must end in `_ts` |
| V4 | R4 | If column is a PK or FK to another resource → column must end in `_id` |
| V5 | R5 | If column is a value-identifier (SSN, MRN, phone, license, passport) → column must end in `_num` |
| V6 | R6 | If column is a coded value or categorical → column must end in `_cd` |
| V7 | R7 | If column is a numeric count → column must end in `_ct` |
| V8 | R8 | If column name would be ambiguous across resources → prefix with resource type |
| V9 | General | Column name must be lowercase snake_case, no spaces or special characters |
| V10 | General | Boolean columns must start with `is_` and end with `_flag` |
