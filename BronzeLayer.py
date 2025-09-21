# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists hotels;

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import date

# COMMAND ----------

current_date = date.today()
landing_bucket_path = f's3://landing-data-bucket-5f593a/hotels/hotels_dataset_{current_date}.parquet'

# COMMAND ----------

df = spark.read.parquet(landing_bucket_path) 

# COMMAND ----------

day = current_date.day
month = current_date.month
year = current_date.year
raw_bucket_path = f's3://raw-data-bucket-5f593a/raw_hotels/year={year}/month={month}/day={day}'
df.write.mode("overwrite").format("parquet").option("path",raw_bucket_path).save()