# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: FHIR Resource Flattening
# MAGIC
# MAGIC This notebook flattens the nested FHIR Bundles from Bronze into separate Silver tables.
# MAGIC Each resource type (Patient, Encounter, Condition, etc.) is written to its own Delta table.

# COMMAND ----------

from pyspark.sql.functions import col, explode, current_timestamp, to_date, to_timestamp, when, lit
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType, DateType, TimestampType

# COMMAND ----------

BRONZE_TABLE = "workspace.default.bronze_synthea_raw"
SILVER_SCHEMA = "workspace.default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze and Explode Bundle Entries

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

# Explode the entry array to get one row per FHIR resource
entries_df = bronze_df.select(
    explode(col("raw_bundle.entry")).alias("entry"),
    col("source_file_nm")
)

# Extract the resource object and resourceType
resources_df = entries_df.select(
    col("entry.resource").alias("resource"),
    col("entry.resource.resourceType").alias("resource_type"),
    col("source_file_nm")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Write Silver Table

# COMMAND ----------

def write_silver_table(df, table_name, columns):
    """
    Selects and renames columns, then writes to a Silver Delta table.
    """
    silver_df = df.select([col(c).alias(name) for c, name in columns])
    
    # Add pipeline metadata
    silver_df = silver_df.withColumn("ingest_ts", current_timestamp()) \
                         .withColumn("source_file_nm", col("source_file_nm"))
    
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.patient

# COMMAND ----------

patient_df = resources_df.filter(col("resource_type") == "Patient")

patient_columns = [
    ("resource.id", "patient_id"),
    ("resource.identifier[type='MR'].value", "medical_record_num"),
    ("resource.identifier[type='SS'].value", "social_security_num"),
    ("resource.identifier[type='DL'].value", "drivers_license_num"),
    ("resource.identifier[type='PPN'].value", "passport_num"),
    ("resource.name[0].given[0]", "first_nm"),
    ("resource.name[0].family", "last_nm"),
    ("resource.name[0].prefix[0]", "prefix_nm"),
    ("resource.gender", "gender_cd"),
    ("resource.birthDate", "birth_dt"),
    ("resource.extension[url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'].extension[url='ombCategory'].valueCoding.display", "race_cd"),
    ("resource.extension[url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'].extension[url='ombCategory'].valueCoding.display", "ethnicity_cd"),
    ("resource.extension[url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex'].valueCode", "birth_sex_cd"),
    ("resource.maritalStatus.text", "marital_status_cd"),
    ("resource.communication[0].language.text", "language_cd"),
    ("resource.telecom[system='phone'].value", "phone_num"),
    ("resource.address[0].line[0]", "patient_address_line"),
    ("resource.address[0].city", "patient_city"),
    ("resource.address[0].state", "patient_state_cd"),
    ("resource.address[0].postalCode", "patient_postal_cd"),
    ("resource.address[0].country", "patient_country_cd"),
    ("resource.address[0].extension[url='http://hl7.org/fhir/StructureDefinition/geolocation'].extension[url='latitude'].valueDecimal", "patient_lat"),
    ("resource.address[0].extension[url='http://hl7.org/fhir/StructureDefinition/geolocation'].extension[url='longitude'].valueDecimal", "patient_long"),
    ("resource.multipleBirthBoolean", "is_multiple_birth_flag"),
    ("resource.extension[url='http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName'].valueString", "mothers_maiden_nm"),
]

write_silver_table(patient_df, "silver_patient", patient_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.encounter

# COMMAND ----------

encounter_df = resources_df.filter(col("resource_type") == "Encounter")

encounter_columns = [
    ("resource.id", "encounter_id"),
    ("resource.subject.reference", "patient_id"),
    ("resource.status", "status_cd"),
    ("resource.class.code", "class_cd"),
    ("resource.class.system", "class_system_cd"),
    ("resource.type[0].coding[0].code", "encounter_type_cd"),
    ("resource.type[0].coding[0].display", "encounter_type_desc"),
    ("resource.type[0].text", "encounter_type_text"),
    ("resource.period.start", "period_start_ts"),
    ("resource.period.end", "period_end_ts"),
    ("resource.reasonCode[0].coding[0].code", "reason_cd"),
    ("resource.reasonCode[0].coding[0].display", "reason_desc"),
    ("resource.participant[0].individual.reference", "practitioner_id"),
    ("resource.participant[0].individual.display", "practitioner_nm"),
    ("resource.location[0].location.reference", "location_id"),
    ("resource.location[0].location.display", "location_nm"),
    ("resource.serviceProvider.reference", "organization_id"),
    ("resource.serviceProvider.display", "organization_nm"),
]

write_silver_table(encounter_df, "silver_encounter", encounter_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.condition

# COMMAND ----------

condition_df = resources_df.filter(col("resource_type") == "Condition")

condition_columns = [
    ("resource.id", "condition_id"),
    ("resource.subject.reference", "patient_id"),
    ("resource.encounter.reference", "encounter_id"),
    ("resource.clinicalStatus.coding[0].code", "clinical_status_cd"),
    ("resource.verificationStatus.coding[0].code", "verification_status_cd"),
    ("resource.category[0].coding[0].code", "category_cd"),
    ("resource.code.coding[0].code", "code_cd"),
    ("resource.code.coding[0].system", "code_system_cd"),
    ("resource.code.coding[0].display", "code_desc"),
    ("resource.code.text", "code_text"),
    ("resource.onsetDateTime", "onset_ts"),
    ("resource.abatementDateTime", "abatement_ts"),
    ("resource.recordedDate", "recorded_dt"),
]

write_silver_table(condition_df, "silver_condition", condition_columns)

# Add derived is_active_flag
spark.sql(f"""
    ALTER TABLE {SILVER_SCHEMA}.silver_condition
    ADD COLUMN is_active_flag BOOLEAN
""")

spark.sql(f"""
    UPDATE {SILVER_SCHEMA}.silver_condition
    SET is_active_flag = (clinical_status_cd = 'active')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.medication_request

# COMMAND ----------

medication_df = resources_df.filter(col("resource_type") == "MedicationRequest")

medication_columns = [
    ("resource.id", "medication_request_id"),
    ("resource.subject.reference", "patient_id"),
    ("resource.encounter.reference", "encounter_id"),
    ("resource.status", "status_cd"),
    ("resource.intent", "intent_cd"),
    ("resource.medicationCodeableConcept.coding[0].code", "medication_cd"),
    ("resource.medicationCodeableConcept.coding[0].system", "medication_system_cd"),
    ("resource.medicationCodeableConcept.coding[0].display", "medication_desc"),
    ("resource.medicationCodeableConcept.text", "medication_text"),
    ("resource.authoredOn", "authored_ts"),
    ("resource.requester.reference", "requester_id"),
    ("resource.requester.display", "requester_nm"),
    ("resource.dosageInstruction[0].text", "dosage_text"),
    ("resource.dosageInstruction[0].asNeededBoolean", "is_as_needed_flag"),
]

write_silver_table(medication_df, "silver_medication_request", medication_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.observation

# COMMAND ----------

observation_df = resources_df.filter(col("resource_type") == "Observation")

observation_columns = [
    ("resource.id", "observation_id"),
    ("resource.subject.reference", "patient_id"),
    ("resource.encounter.reference", "encounter_id"),
    ("resource.status", "status_cd"),
    ("resource.category[0].coding[0].code", "category_cd"),
    ("resource.code.coding[0].code", "code_cd"),
    ("resource.code.coding[0].display", "code_desc"),
    ("resource.effectiveDateTime", "effective_ts"),
    ("resource.issued", "issued_ts"),
    ("resource.valueQuantity.value", "value_num"),
    ("resource.valueQuantity.unit", "value_unit"),
    ("resource.valueQuantity.code", "value_unit_cd"),
    ("resource.valueString", "value_text"),
    ("resource.valueCodeableConcept.text", "value_desc"),
]

write_silver_table(observation_df, "silver_observation", observation_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.claim

# COMMAND ----------

claim_df = resources_df.filter(col("resource_type") == "Claim")

claim_columns = [
    ("resource.id", "claim_id"),
    ("resource.patient.reference", "patient_id"),
    ("resource.status", "status_cd"),
    ("resource.use", "use_cd"),
    ("resource.type.coding[0].code", "claim_type_cd"),
    ("resource.billablePeriod.start", "billable_start_ts"),
    ("resource.billablePeriod.end", "billable_end_ts"),
    ("resource.created", "created_ts"),
    ("resource.provider.reference", "provider_id"),
    ("resource.provider.display", "provider_nm"),
    ("resource.facility.reference", "facility_id"),
    ("resource.facility.display", "facility_nm"),
    ("resource.priority.coding[0].code", "priority_cd"),
    ("resource.insurance[0].coverage.display", "insurance_coverage_nm"),
    ("resource.total.value", "total_amt"),
    ("resource.total.currency", "total_currency_cd"),
]

write_silver_table(claim_df, "silver_claim", claim_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

tables = ["silver_patient", "silver_encounter", "silver_condition", 
          "silver_medication_request", "silver_observation", "silver_claim"]

for table in tables:
    count = spark.table(f"{SILVER_SCHEMA}.{table}").count()
    print(f"{table}: {count} rows")