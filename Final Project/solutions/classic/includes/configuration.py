# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# ANSWER
username = "Fuzhou_jia"

# COMMAND ----------

classicPipelinePath = f"/dbacademy/{username}/Movies/classic/"

landingPath = classicPipelinePath + "landing/"
rawPath = classicPipelinePath + "raw/"
bronzePath = classicPipelinePath + "bronze/"
silverPath = classicPipelinePath + "silver/"
silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
goldPath = classicPipelinePath + "gold/"

rawData= [file.path for file in dbutils.fs.ls("dbfs:/dbacademy/Fuzhou_jia/Movies/classic/raw/")]
moviePath= classicPipelinePath+ "movieSilver/"
genresPath= classicPipelinePath+ "genresSilver/"
OriginalLanguagePath=classicPipelinePath+"originalLanguageSilver/"
movie_genres_Path=classicPipelinePath+"movieGenresSilver/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
