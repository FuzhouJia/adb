# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to Bronze Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest Raw Data
# MAGIC 2. Augment the data with Ingestion Metadata
# MAGIC 3. Batch write the augmented data to a Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data
# MAGIC 
# MAGIC Next, we will read files from the source directory and write each line as a string to the Bronze table.
# MAGIC 
# MAGIC 🤠 You should do this as a batch load using `spark.read`
# MAGIC 
# MAGIC Read in using the format, `"text"`, and using the provided schema.

# COMMAND ----------

# ANSWER
kafka_schema = "value STRING"

raw_movie_df = (
    spark.read.format("text").schema(kafka_schema).load(rawPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data
# MAGIC 
# MAGIC 🤓 Each row here is a raw string in JSON format, as would be passed by a stream server like Kafka.

# COMMAND ----------

display(raw_movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata
# MAGIC 
# MAGIC As part of the ingestion process, we record metadata for the ingestion.
# MAGIC 
# MAGIC **EXERCISE:** Add metadata to the incoming raw data. You should add the following columns:
# MAGIC 
# MAGIC - data source (`datasource`), use `"files.training.databricks.com"`
# MAGIC - ingestion time (`ingesttime`)
# MAGIC - status (`status`), use `"new"`
# MAGIC - ingestion date (`ingestdate`)

# COMMAND ----------

# ANSWER 只把jason files gai le
from pyspark.sql.functions import current_timestamp, lit


raw_movie_df = (
  raw_movie_df.select(
     "value",
    lit("json files ").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),

  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table
# MAGIC 
# MAGIC Finally, we write to the Bronze Table.
# MAGIC 
# MAGIC Make sure to write in the correct order (`"datasource"`, `"ingesttime"`, `"value"`, `"status"`, `"p_ingestdate"`).
# MAGIC 
# MAGIC Make sure to use following options:
# MAGIC 
# MAGIC - the format `"delta"`
# MAGIC - using the append mode
# MAGIC - partition by `p_ingestdate`

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

(
    raw_movie_df.select(
        "datasource",
        "ingesttime",
        "value",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore
# MAGIC 
# MAGIC The table should be named `health_tracker_classic_bronze`.

# COMMAND ----------

# ANSWER
spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table
# MAGIC 
# MAGIC Run this query to display the contents of the Classic Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test
# MAGIC 
# MAGIC SELECT COUNT(value) FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purge Raw File Path
# MAGIC 
# MAGIC We have loaded the raw files using batch loading, whereas with the Plus pipeline we used Streaming.
# MAGIC 
# MAGIC The impact of this is that batch does not use checkpointing and therefore does not know which files have been ingested.
# MAGIC 
# MAGIC We need to manually purge the raw files that have been loaded.

# COMMAND ----------

##为了不重新导入，暂时删
dbutils.fs.rm(rawPath, recurse=True)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
