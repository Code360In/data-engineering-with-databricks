# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

def load_tables():
  spark.sql(f"""
  CREATE OR REPLACE TABLE {database}.events
  DEEP CLONE delta.`{source_tables}/events`
  """)

  spark.sql(f"""
    CREATE OR REPLACE TABLE {database}.sales
    DEEP CLONE delta.`{source_tables}/sales`
  """)

  spark.sql(f"""
    CREATE OR REPLACE TABLE {database}.users
    DEEP CLONE delta.`{source_tables}/users`
  """)

  spark.sql(f"""
    CREATE OR REPLACE TABLE {database}.transactions
    DEEP CLONE delta.`{source_tables}/transactions`
  """)

load_tables()

