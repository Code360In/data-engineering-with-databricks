# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

def load_historical():
  spark.sql(f"""
  CREATE OR REPLACE TABLE events AS
  SELECT * FROM parquet.`{Paths.source}/events/events.parquet`
  """)
  
  spark.sql(f"""
  CREATE OR REPLACE TABLE users AS
  SELECT *, current_timestamp() updated FROM parquet.`{Paths.source}/users/users.parquet`
  """) 
  
  spark.sql(f"""
  CREATE OR REPLACE TABLE sales AS
  SELECT * FROM parquet.`{Paths.source}/sales/sales.parquet`
  """)
  

# COMMAND ----------

load_historical()

