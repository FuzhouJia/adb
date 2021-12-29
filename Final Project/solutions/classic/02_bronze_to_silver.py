# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver table
# MAGIC 
# MAGIC We need to perform some transformations on the data to move it from bronze to silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest raw data using composable functions
# MAGIC 1. Use composable functions to write to the Bronze table
# MAGIC 1. Develop the Bronze to Silver Step
# MAGIC    - Extract and transform the raw string to columns
# MAGIC    - Quarantine the bad data
# MAGIC    - Load clean data into the Silver table
# MAGIC 1. Update the status of records in the Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write Batch to a Bronze Table
# MAGIC 
# MAGIC **Exercise:** Use the function `batch_writer` to ingest the newly arrived
# MAGIC data.
# MAGIC 
# MAGIC **Note**: you will need to begin the write with the `.save()` method on
# MAGIC your writer.
# MAGIC 
# MAGIC 🤖 **Be sure to partition on `p_ingestdate`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Bronze Table
# MAGIC 
# MAGIC If you have ingested 16 hours you should see 160 records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step
# MAGIC 
# MAGIC Let's start the Bronze to Silver step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records
# MAGIC 
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"new"`.

# COMMAND ----------

# ANSWER

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

bronzeDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Nested JSON from the Bronze Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Extract the Nested JSON from the `value` column
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Use `pyspark.sql` functions to extract the `"value"` column as a new
# MAGIC column `"nested_json"`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import from_json

json_schema = """
   BackdropUrl STRING,
    Budget STRING,
    CreatedBy STRING,
    CreatedDate STRING,
    Id STRING,
    ImdbUrl STRING,
    OriginalLanguage STRING,
    Overview STRING,
    PosterUrl STRING,
    Price STRING,
    ReleaseDate STRING,
    Revenue STRING,
    RunTime STRING,
    Tagline STRING,
    Title STRING,
    TmdbUrl STRING,
    UpdatedBy STRING,
    UpdatedDate STRING,
    genres STRING
"""

bronzeAugmentedDF = bronzeDF.withColumn(
    "nested_json", from_json(col("value"), json_schema)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Silver DataFrame by Unpacking the `nested_json` Column
# MAGIC 
# MAGIC Unpacking a JSON column means to flatten the JSON and include each top level attribute
# MAGIC as its own column.
# MAGIC 
# MAGIC 🚨 **IMPORTANT** Be sure to include the `"value"` column in the Silver DataFrame
# MAGIC because we will later use it as a unique reference to each record in the
# MAGIC Bronze table

# COMMAND ----------

# ANSWER
silver_movie = bronzeAugmentedDF.select("value", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC The DataFrame `silver_health_tracker` should now have the following schema:
# MAGIC 
# MAGIC ```
# MAGIC value: string
# MAGIC BackdropUrl:string
# MAGIC Budget:string
# MAGIC CreatedBy:string
# MAGIC CreatedDate:string
# MAGIC Id:string
# MAGIC ImdbUrl:string
# MAGIC OriginalLanguage:string
# MAGIC Overview:string
# MAGIC PosterUrl:string
# MAGIC Price:string
# MAGIC ReleaseDate:string
# MAGIC Revenue:string
# MAGIC RunTime:string
# MAGIC Tagline:string
# MAGIC Title:string
# MAGIC TmdbUrl:string
# MAGIC UpdatedBy:string
# MAGIC UpdatedDate:string
# MAGIC genres:string
# MAGIC ```
# MAGIC 
# MAGIC 💪🏼 Remember, the function `_parse_datatype_string` converts a DDL format schema string into a Spark schema.

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_movie.schema == _parse_datatype_string(
    """
  value STRING,
  BackdropUrl STRING,
  Budget STRING,
  CreatedBy STRING,
  CreatedDate STRING,
  Id STRING,
  ImdbUrl STRING,
  OriginalLanguage STRING,
  Overview STRING,
  PosterUrl STRING,
  Price STRING,
  ReleaseDate STRING,
  Revenue STRING,
  RunTime STRING,
  Tagline STRING,
  Title STRING,
  TmdbUrl STRING,  
  UpdatedBy STRING,
  UpdatedDate STRING,
  genres STRING
"""
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Data
# MAGIC 
# MAGIC 1. Create a column `p_eventdate DATE` from the column `time`.
# MAGIC 1. Rename the column `time` to `eventtime`.
# MAGIC 1. Cast the `device_id` as an integer.
# MAGIC 1. Include only the following columns in this order:
# MAGIC    1. `value`
# MAGIC    1. `device_id`
# MAGIC    1. `steps`
# MAGIC    1. `eventtime`
# MAGIC    1. `name`
# MAGIC    1. `p_eventdate`
# MAGIC 
# MAGIC 💪🏼 Remember that we name the new column `p_eventdate` to indicate
# MAGIC that we are partitioning on this column.
# MAGIC 
# MAGIC 🕵🏽‍♀️ Remember that we are keeping the `value` as a unique reference to values
# MAGIC in the Bronze table.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

silver_movie = silver_movie.select(
    "value",
    "BackdropUrl",
    "Budget",
    "CreatedBy",
    col("CreatedDate").cast("date"),
    col("Id").cast("integer").alias("movie_id"),
    "ImdbUrl",
    "OriginalLanguage",
    "Overview",
    "PosterUrl",
    "Price",
    col("ReleaseDate").cast("date"),
    "Revenue",
    "RunTime",
    "Tagline",
    "Title",
    "TmdbUrl",
    "UpdatedBy",
    "UpdatedDate",
    "genres",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC The DataFrame `silver_health_tracker_data_df` should now have the following schema:
# MAGIC 
# MAGIC ```
# MAGIC value: string
# MAGIC device_id: integer
# MAGIC heartrate: double
# MAGIC eventtime: timestamp
# MAGIC name: string
# MAGIC p_eventdate: date```
# MAGIC 
# MAGIC 💪🏼 Remember, the function `_parse_datatype_string` converts a DDL format schema string into a Spark schema.

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_movie.schema == _parse_datatype_string(
    """
  value STRING,
  BackdropUrl STRING,
  Budget STRING,
  CreatedBy STRING,
  CreatedDate DATE,
  movie_id INTEGER,
  ImdbUrl STRING,
  OriginalLanguage STRING,
  Overview STRING,
  PosterUrl STRING,
  Price STRING,
  ReleaseDate Date,
  Revenue STRING,
  RunTime STRING,
  Tagline STRING,
  Title STRING,
  TmdbUrl STRING,  
  UpdatedBy STRING,
  UpdatedDate STRING,
  genres STRING
"""
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the Bad Data
# MAGIC 
# MAGIC Recall that at step, `00_ingest_raw`, we identified that some records were coming in
# MAGIC with device_ids passed as uuid strings instead of string-encoded integers.
# MAGIC Our Silver table stores device_ids as integers so clearly there is an issue
# MAGIC with the incoming data.
# MAGIC 
# MAGIC In order to properly handle this data quality issue, we will quarantine
# MAGIC the bad records for later processing.

# COMMAND ----------

# MAGIC %md
# MAGIC Check for records that have nulls - compare the output of the following two cells

# COMMAND ----------

silver_movie.count()

# COMMAND ----------

silver_movie.na.drop().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Silver DataFrame

# COMMAND ----------

silver_movie_clean = silver_movie.filter("Runtime >= 0" and "Budget >= 1000000")
silver_movie_quarantine = silver_movie.filter("Budget < 1000000") or ("Runtime < 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records

# COMMAND ----------

display(silver_movie_quarantine)

# COMMAND ----------

silver_movie_clean.count()

# COMMAND ----------

silver_movie_quarantine.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table
# MAGIC 
# MAGIC **EXERCISE:** Batch write `silver_health_tracker_clean` to the Silver table path, `silverPath`.
# MAGIC 
# MAGIC 1. Use format, `"delta"`
# MAGIC 1. Use mode `"append"`.
# MAGIC 1. Do **NOT** include the `value` column.
# MAGIC 1. Partition by `"p_eventdate"`.

# COMMAND ----------

# ANSWER
(
  silver_movie_clean.select(
    "BackdropUrl",
    "Budget",
    "CreatedBy",
    "CreatedDate",
    "movie_id",
    "ImdbUrl",
    "OriginalLanguage",
    "Overview",
    "PosterUrl",
    "Price",
    "ReleaseDate",
    "Revenue",
    "RunTime",
    "Tagline",
    "Title",
    "TmdbUrl",
    "UpdatedBy",
    "UpdatedDate"
    )
  .write.format("delta")
  .mode("append")
  .partitionBy("CreatedDate")
  .save(silverPath)  
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS health_tracker_classic_silver
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_classic_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

silverTable = spark.read.table("health_tracker_classic_silver")
expected_schema = """
  device_id INTEGER,
  steps INTEGER,
  eventtime TIMESTAMP,
  name STRING,
  p_eventdate DATE
"""

assert silverTable.schema == _parse_datatype_string(
    expected_schema
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads
# MAGIC 
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 1: Update Clean records
# MAGIC Clean records that have been loaded into the Silver table and should have
# MAGIC    their Bronze table `status` updated to `"loaded"`.
# MAGIC 
# MAGIC 💃🏽 **Hint** You are matching the `value` column in your clean Silver DataFrame
# MAGIC to the `value` column in the Bronze table.

# COMMAND ----------

# ANSWER
from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_health_tracker_clean.withColumn("status", lit("loaded"))

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 2: Update Quarantined records
# MAGIC Quarantined records should have their Bronze table `status` updated to `"quarantined"`.
# MAGIC 
# MAGIC 🕺🏻 **Hint** You are matching the `value` column in your quarantine Silver
# MAGIC DataFrame to the `value` column in the Bronze table.

# COMMAND ----------

# ANSWER
silverAugmented = silver_health_tracker_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
