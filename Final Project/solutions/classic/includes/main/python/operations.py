# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "overwrite",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .option("overwriteSchema","true")
        .partitionBy(partition_column)
    )


# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("RunTime>=0" and  "Budget>=1000000"),
        dataframe.filter("Budget < 1000000") or ("RunTime < 0")
    )

# COMMAND ----------

# ANSWER
def read_batch_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.table("movie_bronze").filter("status = 'new'")


# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)


# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    kafka_schema = "value STRING"
    return spark.read.format("text").schema(kafka_schema).load(rawPath)


# COMMAND ----------

from pyspark.sql.functions import abs,when
def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    json_schema = """
     
   BackdropUrl STRING,
    Budget Double,
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
    RunTime LONG,
    Tagline STRING,
    Title STRING,
    TmdbUrl STRING,
    UpdatedBy STRING,
    UpdatedDate STRING,
    genres STRING

  """

    bronzeAugmentedDF = bronze.withColumn(
        "nested_json", from_json(col("value"), json_schema)
    )

    silver_movie  = bronzeAugmentedDF.select("value", "nested_json.*")

    if not quarantine:
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
    else:
        silver_movie = silver_movie.withColumn("Budget", when(col("Budget") < 1000000, 1000000).when(col("Budget") >= 1000000, "Budget").otherwise(1000000))
        silver_movie = silver_movie.select(
            "value",
            "BackdropUrl",
            col("Budget"),
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
            abs("RunTime").cast("Integer").alias("RunTime"),
            "Tagline",
            "Title",
            "TmdbUrl",
            "UpdatedBy",
            col("UpdatedDate").cast("date").alias("UpdatedDate"),
            "genres",
        )

    return silver_movie


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF


# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        "value",
    lit("json files ").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),

    )


# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status)).dropDuplicates()

    update_match = "bronze.value = dataframe.value"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True

