# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver Layer: FHIR Resource Flattening
# MAGIC
# MAGIC Flattens nested FHIR Bundles from `workspace.fhir_rawz.bundle_base` into separate Silver tables
# MAGIC Each resource type gets its own table.

# COMMAND ----------

from pyspark.sql.functions import col, explode, current_timestamp, when, lit, variant_get, from_json, schema_of_json, schema_of_variant_agg
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, DateType, TimestampType

# COMMAND ----------

# Configuration
BRONZE_TABLE = "workspace.fhir_rawz.bundle_base"
SILVER_SCHEMA = "workspace.fhir_cnfz"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze and Explode Bundle Entries

# COMMAND ----------
resources_df = spark.sql(f"""
    SELECT
        variant_get(e.value, '$.resource',              'variant')  AS resource,
        variant_get(e.value, '$.resource.resourceType', 'string')   AS resource_type,
        b.source_file_nm
    FROM {BRONZE_TABLE} AS b,
    LATERAL variant_explode(b.raw_bundle:entry) AS e
""")

print("Resource count:", resources_df.count())
resources_df.printSchema()

# Cache for performance - not supported on serverless
# resources_df.cache()

# Write exploded resources to a stg (replaces cache)
staging_table_name = f"{SILVER_SCHEMA}.bundle_stg"
resources_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(staging_table_name)

# Read back from the staging table
resources_df = spark.table(staging_table_name)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function: Write Silver Table

# COMMAND ----------

def write_silver_table(df, table_name):
    """Write a DataFrame to a Silver table with ingest metadata."""
    silver_df = df.withColumn("ingest_ts", current_timestamp())
    full_name = f"{SILVER_SCHEMA}.{table_name}"
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_name)
    print(f"Wrote {silver_df.count()} rows to {full_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.patient
# MAGIC
# MAGIC Flatten Patient resources. PHI columns are flagged for masking.

# COMMAND ----------

patient_df = resources_df.filter(col("resource_type") == "Patient").select(
    variant_get(col("resource"), "$.id", "string").alias("patient_id"),
    variant_get(col("resource"), "$.identifier[0].value", "string").alias("medical_record_num"),
    variant_get(col("resource"), "$.name[0].given[0]", "string").alias("first_nm"),
    variant_get(col("resource"), "$.name[0].family", "string").alias("last_nm"),
    variant_get(col("resource"), "$.name[0].prefix[0]", "string").alias("prefix_nm"),
    variant_get(col("resource"), "$.gender", "string").alias("gender_cd"),
    variant_get(col("resource"), "$.birthDate", "string").alias("birth_dt"),
    variant_get(col("resource"), "$.maritalStatus.text", "string").alias("marital_status_cd"),
    variant_get(col("resource"), "$.communication[0].language.text", "string").alias("language_cd"),
    variant_get(col("resource"), "$.telecom[0].value", "string").alias("phone_num"),
    variant_get(col("resource"), "$.address[0].line[0]", "string").alias("patient_address_line"),
    variant_get(col("resource"), "$.address[0].city", "string").alias("patient_city"),
    variant_get(col("resource"), "$.address[0].state", "string").alias("patient_state_cd"),
    variant_get(col("resource"), "$.address[0].postalCode", "string").alias("patient_postal_cd"),
    variant_get(col("resource"), "$.address[0].country", "string").alias("patient_country_cd"),
    variant_get(col("resource"), "$.multipleBirthBoolean", "boolean").alias("is_multiple_birth_flag"),
    col("source_file_nm"),
)

write_silver_table(patient_df, "patient")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.encounter

# COMMAND ----------

encounter_df = resources_df.filter(col("resource_type") == "Encounter").select(
    variant_get(col("resource"), "$.id", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.class.code", "string").alias("class_cd"),
    variant_get(col("resource"), "$.type[0].coding[0].code", "string").alias("encounter_type_cd"),
    variant_get(col("resource"), "$.type[0].coding[0].display", "string").alias("encounter_type_desc"),
    variant_get(col("resource"), "$.type[0].text", "string").alias("encounter_type_text"),
    variant_get(col("resource"), "$.period.start", "string").alias("period_start_ts"),
    variant_get(col("resource"), "$.period.end", "string").alias("period_end_ts"),
    variant_get(col("resource"), "$.reasonCode[0].coding[0].code", "string").alias("reason_cd"),
    variant_get(col("resource"), "$.reasonCode[0].coding[0].display", "string").alias("reason_desc"),
    variant_get(col("resource"), "$.participant[0].individual.reference", "string").alias("practitioner_id"),
    variant_get(col("resource"), "$.participant[0].individual.display", "string").alias("practitioner_nm"),
    variant_get(col("resource"), "$.location[0].location.reference", "string").alias("location_id"),
    variant_get(col("resource"), "$.location[0].location.display", "string").alias("location_nm"),
    variant_get(col("resource"), "$.serviceProvider.reference", "string").alias("organization_id"),
    variant_get(col("resource"), "$.serviceProvider.display", "string").alias("organization_nm"),
    col("source_file_nm"),
)

write_silver_table(encounter_df, "encounter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.condition

# COMMAND ----------

condition_df = resources_df.filter(col("resource_type") == "Condition").select(
    variant_get(col("resource"), "$.id", "string").alias("condition_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.clinicalStatus.coding[0].code", "string").alias("clinical_status_cd"),
    variant_get(col("resource"), "$.verificationStatus.coding[0].code", "string").alias("verification_status_cd"),
    variant_get(col("resource"), "$.category[0].coding[0].code", "string").alias("category_cd"),
    variant_get(col("resource"), "$.code.coding[0].code", "string").alias("code_cd"),
    variant_get(col("resource"), "$.code.coding[0].display", "string").alias("code_desc"),
    variant_get(col("resource"), "$.code.text", "string").alias("code_text"),
    variant_get(col("resource"), "$.onsetDateTime", "string").alias("onset_ts"),
    variant_get(col("resource"), "$.abatementDateTime", "string").alias("abatement_ts"),
    variant_get(col("resource"), "$.recordedDate", "string").alias("recorded_dt"),
    col("source_file_nm"),
).withColumn(
    "is_active_flag",
    when(col("clinical_status_cd") == "active", True).otherwise(False)
)

write_silver_table(condition_df, "condition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.medication_request

# COMMAND ----------

medication_df = resources_df.filter(col("resource_type") == "MedicationRequest").select(
    variant_get(col("resource"), "$.id", "string").alias("medication_request_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.intent", "string").alias("intent_cd"),
    variant_get(col("resource"), "$.medicationCodeableConcept.coding[0].code", "string").alias("medication_cd"),
    variant_get(col("resource"), "$.medicationCodeableConcept.coding[0].display", "string").alias("medication_desc"),
    variant_get(col("resource"), "$.medicationCodeableConcept.text", "string").alias("medication_text"),
    variant_get(col("resource"), "$.authoredOn", "string").alias("authored_ts"),
    variant_get(col("resource"), "$.requester.reference", "string").alias("requester_id"),
    variant_get(col("resource"), "$.requester.display", "string").alias("requester_nm"),
    variant_get(col("resource"), "$.dosageInstruction[0].text", "string").alias("dosage_text"),
    variant_get(col("resource"), "$.dosageInstruction[0].asNeededBoolean", "boolean").alias("is_as_needed_flag"),
    col("source_file_nm"),
)

write_silver_table(medication_df, "medication_request")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.observation

# COMMAND ----------

observation_df = resources_df.filter(col("resource_type") == "Observation").select(
    variant_get(col("resource"), "$.id", "string").alias("observation_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.category[0].coding[0].code", "string").alias("category_cd"),
    variant_get(col("resource"), "$.code.coding[0].code", "string").alias("code_cd"),
    variant_get(col("resource"), "$.code.coding[0].display", "string").alias("code_desc"),
    variant_get(col("resource"), "$.effectiveDateTime", "string").alias("effective_ts"),
    variant_get(col("resource"), "$.issued", "string").alias("issued_ts"),
    variant_get(col("resource"), "$.valueQuantity.value", "double").alias("value_num"),
    variant_get(col("resource"), "$.valueQuantity.unit", "string").alias("value_unit"),
    variant_get(col("resource"), "$.valueString", "string").alias("value_text"),
    variant_get(col("resource"), "$.valueCodeableConcept.text", "string").alias("value_desc"),
    col("source_file_nm"),
)

write_silver_table(observation_df, "observation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.claim

# COMMAND ----------

claim_df = resources_df.filter(col("resource_type") == "Claim").select(
    variant_get(col("resource"), "$.id", "string").alias("claim_id"),
    variant_get(col("resource"), "$.patient.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.use", "string").alias("use_cd"),
    variant_get(col("resource"), "$.type.coding[0].code", "string").alias("claim_type_cd"),
    variant_get(col("resource"), "$.billablePeriod.start", "string").alias("billable_start_ts"),
    variant_get(col("resource"), "$.billablePeriod.end", "string").alias("billable_end_ts"),
    variant_get(col("resource"), "$.created", "string").alias("created_ts"),
    variant_get(col("resource"), "$.provider.reference", "string").alias("provider_id"),
    variant_get(col("resource"), "$.provider.display", "string").alias("provider_nm"),
    variant_get(col("resource"), "$.facility.reference", "string").alias("facility_id"),
    variant_get(col("resource"), "$.facility.display", "string").alias("facility_nm"),
    variant_get(col("resource"), "$.priority.coding[0].code", "string").alias("priority_cd"),
    variant_get(col("resource"), "$.insurance[0].coverage.display", "string").alias("insurance_coverage_nm"),
    variant_get(col("resource"), "$.total.value", "double").alias("total_amt"),
    variant_get(col("resource"), "$.total.currency", "string").alias("total_currency_cd"),
    col("source_file_nm"),
)

write_silver_table(claim_df, "claim")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.explanation_of_benefit

# COMMAND ----------

eob_df = resources_df.filter(col("resource_type") == "ExplanationOfBenefit").select(
    variant_get(col("resource"), "$.id", "string").alias("eob_id"),
    variant_get(col("resource"), "$.patient.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.claim.reference", "string").alias("claim_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.use", "string").alias("use_cd"),
    variant_get(col("resource"), "$.type.coding[0].code", "string").alias("eob_type_cd"),
    variant_get(col("resource"), "$.outcome", "string").alias("outcome_cd"),
    variant_get(col("resource"), "$.billablePeriod.start", "string").alias("billable_start_ts"),
    variant_get(col("resource"), "$.billablePeriod.end", "string").alias("billable_end_ts"),
    variant_get(col("resource"), "$.created", "string").alias("created_ts"),
    variant_get(col("resource"), "$.insurer.display", "string").alias("insurer_nm"),
    variant_get(col("resource"), "$.provider.reference", "string").alias("provider_id"),
    variant_get(col("resource"), "$.facility.reference", "string").alias("facility_id"),
    variant_get(col("resource"), "$.facility.display", "string").alias("facility_nm"),
    variant_get(col("resource"), "$.total[0].amount.value", "double").alias("total_submitted_amt"),
    variant_get(col("resource"), "$.payment.amount.value", "double").alias("payment_amt"),
    col("source_file_nm"),
)

write_silver_table(eob_df, "explanation_of_benefit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.care_team

# COMMAND ----------

care_team_df = resources_df.filter(col("resource_type") == "CareTeam").select(
    variant_get(col("resource"), "$.id", "string").alias("care_team_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.period.start", "string").alias("period_start_ts"),
    variant_get(col("resource"), "$.reasonCode[0].coding[0].code", "string").alias("reason_cd"),
    variant_get(col("resource"), "$.reasonCode[0].coding[0].display", "string").alias("reason_desc"),
    variant_get(col("resource"), "$.managingOrganization[0].reference", "string").alias("organization_id"),
    variant_get(col("resource"), "$.managingOrganization[0].display", "string").alias("organization_nm"),
    col("source_file_nm"),
)

write_silver_table(care_team_df, "care_team")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.care_plan

# COMMAND ----------

care_plan_df = resources_df.filter(col("resource_type") == "CarePlan").select(
    variant_get(col("resource"), "$.id", "string").alias("care_plan_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.intent", "string").alias("intent_cd"),
    variant_get(col("resource"), "$.category[0].coding[0].code", "string").alias("category_cd"),
    variant_get(col("resource"), "$.period.start", "string").alias("period_start_ts"),
    variant_get(col("resource"), "$.careTeam[0].reference", "string").alias("care_team_id"),
    col("source_file_nm"),
)

write_silver_table(care_plan_df, "care_plan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.diagnostic_report

# COMMAND ----------

diagnostic_df = resources_df.filter(col("resource_type") == "DiagnosticReport").select(
    variant_get(col("resource"), "$.id", "string").alias("diagnostic_report_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.category[0].coding[0].code", "string").alias("category_cd"),
    variant_get(col("resource"), "$.code.coding[0].code", "string").alias("code_cd"),
    variant_get(col("resource"), "$.code.coding[0].display", "string").alias("code_desc"),
    variant_get(col("resource"), "$.effectiveDateTime", "string").alias("effective_ts"),
    variant_get(col("resource"), "$.issued", "string").alias("issued_ts"),
    variant_get(col("resource"), "$.performer[0].reference", "string").alias("performer_id"),
    variant_get(col("resource"), "$.performer[0].display", "string").alias("performer_nm"),
    variant_get(col("resource"), "$.presentedForm[0].data", "string").alias("presented_form_data"),
    col("source_file_nm"),
)

write_silver_table(diagnostic_df, "diagnostic_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.document_reference

# COMMAND ----------

document_df = resources_df.filter(col("resource_type") == "DocumentReference").select(
    variant_get(col("resource"), "$.id", "string").alias("document_reference_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.context.encounter[0].reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.type.coding[0].code", "string").alias("doc_type_cd"),
    variant_get(col("resource"), "$.type.coding[0].display", "string").alias("doc_type_desc"),
    variant_get(col("resource"), "$.date", "string").alias("document_ts"),
    variant_get(col("resource"), "$.author[0].reference", "string").alias("author_id"),
    variant_get(col("resource"), "$.author[0].display", "string").alias("author_nm"),
    variant_get(col("resource"), "$.custodian.reference", "string").alias("custodian_id"),
    variant_get(col("resource"), "$.custodian.display", "string").alias("custodian_nm"),
    variant_get(col("resource"), "$.content[0].attachment.contentType", "string").alias("content_type_cd"),
    variant_get(col("resource"), "$.content[0].attachment.data", "string").alias("content_data"),
    col("source_file_nm"),
)

write_silver_table(document_df, "document_reference")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.immunization

# COMMAND ----------

immunization_df = resources_df.filter(col("resource_type") == "Immunization").select(
    variant_get(col("resource"), "$.id", "string").alias("immunization_id"),
    variant_get(col("resource"), "$.patient.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.vaccineCode.coding[0].code", "string").alias("vaccine_cd"),
    variant_get(col("resource"), "$.vaccineCode.coding[0].display", "string").alias("vaccine_desc"),
    variant_get(col("resource"), "$.occurrenceDateTime", "string").alias("occurrence_ts"),
    variant_get(col("resource"), "$.primarySource", "boolean").alias("is_primary_source_flag"),
    variant_get(col("resource"), "$.location.reference", "string").alias("location_id"),
    variant_get(col("resource"), "$.location.display", "string").alias("location_nm"),
    col("source_file_nm"),
)

write_silver_table(immunization_df, "immunization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fhir_cnfz.procedure

# COMMAND ----------

procedure_df = resources_df.filter(col("resource_type") == "Procedure").select(
    variant_get(col("resource"), "$.id", "string").alias("procedure_id"),
    variant_get(col("resource"), "$.subject.reference", "string").alias("patient_id"),
    variant_get(col("resource"), "$.encounter.reference", "string").alias("encounter_id"),
    variant_get(col("resource"), "$.status", "string").alias("status_cd"),
    variant_get(col("resource"), "$.code.coding[0].code", "string").alias("code_cd"),
    variant_get(col("resource"), "$.code.coding[0].display", "string").alias("code_desc"),
    variant_get(col("resource"), "$.performedPeriod.start", "string").alias("performed_start_ts"),
    variant_get(col("resource"), "$.performedPeriod.end", "string").alias("performed_end_ts"),
    variant_get(col("resource"), "$.location.reference", "string").alias("location_id"),
    variant_get(col("resource"), "$.location.display", "string").alias("location_nm"),
    variant_get(col("resource"), "$.reasonReference[0].reference", "string").alias("reason_id"),
    col("source_file_nm"),
)

write_silver_table(procedure_df, "procedure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

tables = ["patient", "encounter", "condition", "medication_request", "observation",
          "claim", "explanation_of_benefit", "care_team", "care_plan",
          "diagnostic_report", "document_reference", "immunization", "procedure"]

for t in tables:
    try:
        cnt = spark.table(f"{SILVER_SCHEMA}.{t}").count()
        print(f"{SILVER_SCHEMA}.{t}: {cnt} rows")
    except Exception as e:
        print(f"{SILVER_SCHEMA}.{t}: ERROR - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# resources_df.unpersist() - not supported in serverless
spark.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.bundle_stg")
