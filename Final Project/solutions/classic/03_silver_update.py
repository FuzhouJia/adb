# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table Updates
# MAGIC 
# MAGIC We have processed data from the Bronze table to the Silver table.
# MAGIC 
# MAGIC We now need to do some updates to ensure high data quality in the Silver
# MAGIC table. Because batch loading has no mechanism for checkpointing, we will
# MAGIC need a way to load _only the new records_ from the Bronze table.
# MAGIC 
# MAGIC We also need to deal with the quarantined records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Update the `read_batch_bronze` function to read only new records
# MAGIC 1. Fix the bad quarantined records from the Bronze table
# MAGIC 1. Write the repaired records to the Silver table

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

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Delta Architecture
# MAGIC Next, we demonstrate everything we have built up to this point in our
# MAGIC Delta Architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Raw to Bronze Pipeline

# COMMAND ----------

ingest_classic_data()
kafka_schema = "value STRING"

raw_movie_df = (
    spark.read.format("text").schema(kafka_schema).load(rawPath)
)

transformedRawDF = transform_raw(raw_movie_df)##operation cmd9, change to the same as rtb cmd14
rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="ingestdate"
)

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Purge Raw File Path
# MAGIC 
# MAGIC Manually purge the raw files that have already been loaded.

# COMMAND ----------

# ANSWER
#dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze to Silver Pipeline
# MAGIC 
# MAGIC 
# MAGIC In the previous notebook, to ingest only the new data we ran
# MAGIC 
# MAGIC ```
# MAGIC bronzeDF = (
# MAGIC   spark.read
# MAGIC   .table("health_tracker_classic_bronze")
# MAGIC   .filter("status = 'new'")
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC **Exercise**
# MAGIC 
# MAGIC Update the function `read_batch_bronze` in the
# MAGIC `includes/main/python/operations` file so that it reads only the new
# MAGIC files in the Bronze table.

# COMMAND ----------

bronzeDF = (
  spark.read
  .table("movie_bronze")
  .filter("status = 'new'")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ♨️ After updating the `read_batch_bronze` function, re-source the
# MAGIC `includes/main/python/operations` file to include your updates by running the cell below.

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

transformedBronzeDF = transform_bronze(bronzeDF)

(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(
    transformedBronzeDF
)



# COMMAND ----------

 # silverQuarantineDF=silverQuarantineDF.dropDuplicates()#为什么这个不行，需要在cmd20function家dropdup，operation10;

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanDF, partition_column="UpdatedDate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform a Visual Verification of the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze -- 为什么shigetable，

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"quarantined"`.

# COMMAND ----------

# ANSWER

bronzeQuarantinedDF = spark.read.table("movie_bronze").filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Quarantined Records
# MAGIC 
# MAGIC This applies the standard bronze table transformations.

# COMMAND ----------

bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
    "quarantine"
)
display(bronzeQuarTransDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: fix runtime with absolute value of the orginal negative value, and change the budget to 1 million
# MAGIC 
# MAGIC We do this to retrieve the correct device id associated with each user.

# COMMAND ----------

from pyspark.sql.functions import abs, when

silverCleanedDF_1 = transform_bronze(bronzeQuarTransDF,True)
  
display(silverCleanedDF_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Batch Write the Repaired (formerly Quarantined) Records to the Silver Table
# MAGIC 
# MAGIC After loading, this will also update the status of the quarantined records
# MAGIC to `loaded`.

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanedDF_1, partition_column="UpdatedDate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanedDF_1, "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records
# MAGIC 
# MAGIC If the update was successful, there should be no quarantined records
# MAGIC in the Bronze table.

# COMMAND ----------

display(bronzeQuarantinedDF)


# COMMAND ----------

display(silverCleanedDF_1)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
