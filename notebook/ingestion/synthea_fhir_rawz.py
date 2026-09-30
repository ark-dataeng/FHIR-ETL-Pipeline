# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze Layer: Synthea FHIR Ingestion
# MAGIC
# MAGIC This notebook uses Auto Loader to ingest raw Synthea FHIR bundles from a Unity Catalog Volume.
# MAGIC The entire JSON record is stored as a VARIANT column to preserve the nested structure.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, parse_json

# COMMAND ----------

# Configuration
SOURCE_PATH = "/Volumes/workspace/default/data/fhir/"
CHECKPOINT_PATH = "/Volumes/workspace/default/data/checkpoints/"
BRONZE_TABLE = "workspace.fhir_rawz.bundle_base"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Raw FHIR Bundles
# MAGIC
# MAGIC Auto Loader detects new files and processes them incrementally.
# MAGIC The `singleVariantColumn` option stores the entire JSON as a VARIANT type.

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("singleVariantColumn", "raw_bundle")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(SOURCE_PATH)
)

# COMMAND ----------

# Write to Bronze Delta table
(
    bronze_df
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("source_file_nm", col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Ingestion

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(5))