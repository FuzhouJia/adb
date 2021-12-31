# Databricks notebook source

from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
from pyspark.sql.functions import from_unixtime, dayofmonth, month, hour
from delta import DeltaTable
from datetime import datetime
import time
MOVIE_DELTA = "movies.delta"
genres_data ="genres.delta"
##old
CLASSIC_DATA = "classic_data_2020_h1.snappy.parquet"
CLASSIC_DELTA = "classic_data_2020_h1.delta"

# COMMAND ----------

## File path and read json
from pyspark.sql.functions import explode
def prepare_activity_data(landingPath) -> bool:
   file_path = [file.path for file in dbutils.fs.ls("/FileStore/") if "movie_" in file.path]
   
   movieIngest = (
    spark.read.option("multiline","true")
    .option("inferSchema", "true")
    .format("json")
    .load(file_path)
    #.withColumn("time", from_unixtime("time"))
    .select(explode("movie").alias("movies"))
    .write.format("delta")
    .save(landingPath + MOVIE_DELTA)
  )


# COMMAND ----------

def ingest_classic_data(hours: int = 1) -> bool:
  MOVIE_DELTA = "movies.delta"
  next_batch = spark.read.format("delta").load(landingPath + MOVIE_DELTA)
  next_batch = (next_batch.select("movies.*"))
  file_name = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
  (next_batch.write.format("json").save(rawPath + file_name))
  #move file out of directory and rename
  i=1
  for file in dbutils.fs.ls(rawPath + file_name):
    if "part" in file.path:
      dbutils.fs.mv(file.path, rawPath + f"jsonpart {i}" + ".txt")
      i = i + 1 
  dbutils.fs.rm(rawPath + file_name, recurse=True)
  return True


##old 没改
def untilStreamIsReady(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True


# COMMAND ----------

##from operation cmd1
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

def ingest_batch_raw(Path: str) -> DataFrame:
    return spark.read.format("json").option("multiline", "true").option("inferSchema", "true").load(Path)
