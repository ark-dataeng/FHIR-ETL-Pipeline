# FHIR Healthcare Data ETL Pipeline

A Databricks-based medallion architecture (Bronze -> Silver -> Gold) ETL pipeline for processing FHIR (Fast Healthcare Interoperability Resources) healthcare data, with accompanying data design documentation and column naming standards.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Design](#data-design)
- [Column Naming Standards](#column-naming-standards)
- [License](#license)

---

## Overview

This project implements an end-to-end ETL pipeline for FHIR healthcare data on Databricks using Delta Lake. The pipeline ingests raw FHIR resources (JSON) into a **Bronze** layer, refines and conforms them into a **Silver** layer, and produces analytics-ready **Gold** tables for downstream reporting, ML, and BI consumption.

**Key features:**

- Medallion architecture (Bronze -> Silver -> Gold) on Delta Lake
- Support for core FHIR resources (Patient, Encounter, Observation, Condition, MedicationRequest, etc.)
- Parameterized notebooks for data ingestion
- Column naming standards applied consistently across layers
- Data design document covering column information for each Silver layer table


**What is to be added:**
- Architecture diagram and entity relationship model across FHIR resource types
- Schema evolution and data quality enforcement via Delta constraints and DDL notebooks
- Data ingestion jobs that run on a regular schedule. Currently, the notebooks must be triggered manually or by Auto Loader
- A finalized gold layer that is ready for consumption (with data quality standards and PHI/PII handling and masking)
- A consistent CI/CD process via Terraform or some other tool, as well as stored environment variables
- Partitioning, Z-ordering, and SCD strategies
- Data quality expectations and quarantine patterns, with Deequ as a start
- Testing and monitoring of data flow and quality

---

## Architecture

<!-- TODO: Insert architecture diagram (e.g., docs/architecture.png) showing FHIR Source -> Bronze -> Silver -> Gold flow -->

See [`docs/architecture.md`](docs/ARCHITECTURE.md) for the full architecture diagram, design decisions, and layer responsibilities.

### Layer summary

| Layer  | Purpose | Characteristics |
|--------|---------|-----------------|
| Bronze | Raw, immutable ingest of FHIR JSON | Append-only, schema-on-read, ingestion metadata |
| Silver | Parsed, deduplicated, conformed FHIR resources | Typed columns, DQ rules, SCD Type 2 where applicable |
| Gold   | Dimensional models and analytics marts | Star schemas, aggregated KPIs, business logic |

For full details, see the data design document below.

---

## Data Design

The complete data design document is available at [`docs/data_design`](docs/data-design/Synthea_FHIR_Silver_Layer_Data_Design_Document.ods). It covers:

- **Source systems** and FHIR version (R4)
- **Grain definitions** for each Silver table (Gold TBD)
- **Datatype information** for each Silver table (Gold TBD)

### Layer schemas

| Layer  | Schema (Unity Catalog) | Example Table           |
|--------|------------------------|-------------------------|
| Bronze | `fhir_rawz`            | `fhir_rawz.bundle_base` |
| Silver | `fhir_cnfz`            | `fhir_cnfz.patient` |
| Gold   | `fhir_pubz`            | `fhir_pubz.patient_vw` |

---

## Column Naming Standards

All columns adhere to the conventions defined in [`docs/naming_conventions.md`](docs/NAMING_CONVENTIONS.md). Summary:

| Rule | Convention | Example |
|------|-----------|---------|
| Case | `snake_case` | `patient_id` |
| Primary key | `<entity>_id` | `encounter_id` |
| Foreign key | `<referenced_entity>_id` | `patient_id` |
| Boolean | `is_` / `has_` prefix | `is_active`, `has_insurance` |
| Timestamp | `_ts` suffix (UTC) | `created_ts`, `ingested_ts` |
| Date | `_dt` suffix | `birth_dt`, `service_dt` |
| Code fields | `_code` suffix | `gender_code`, `status_code` |
| Display text | `_desc` suffix | `gender_desc`, `status_desc` |
| Audit columns | `_by`, `_ts` suffixes | `created_by`, `updated_ts` |
| No reserved words | Avoid SQL keywords | `class` → `encounter_class` |

**Mandatory audit columns** on every Silver/Gold table:

- `ingested_ts` — Bronze ingest time
- `updated_ts` — last modification time
- `source_system` — originating system identifier
- `record_hash` — SHA-256 hash of business columns for change detection


---

## License

While the primary purpose of this project is not for outside use, it is nevertheless licensed under the [MIT License](LICENSE) — update as appropriate for your organization. Note that this pipeline processes **PHI**, so ensure your deployment complies with HIPAA, GDPR, and your organization's data governance policies.

---

## References

- [FHIR R4 Specification](https://hl7.org/fhir/R4/)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

---

**Maintainers:** _Arianna Kirby_
**Last updated:** _2026-09-30_
