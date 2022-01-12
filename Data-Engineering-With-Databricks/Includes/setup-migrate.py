# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

def migrate_historical():
  spark.sql(f"""
  CREATE OR REPLACE TABLE sales
  LOCATION "{Paths.sales_table_path}" AS
  SELECT * FROM parquet.`{Paths.source}/sales/sales.parquet`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE users
  LOCATION "{Paths.users_table_path}" AS
  SELECT current_timestamp() updated, *
  FROM parquet.`{Paths.source}/users/users.parquet`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE events_clean
  LOCATION "{Paths.events_clean_table_path}" AS
  SELECT * FROM parquet.`{Paths.source}/events/events.parquet`
  """)

# COMMAND ----------

if mode != "clean":
  migrate_historical()


