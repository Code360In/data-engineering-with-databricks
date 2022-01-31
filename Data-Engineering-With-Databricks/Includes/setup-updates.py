# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

def load_historical():
  spark.sql(f"""
  CREATE OR REPLACE TABLE sales
  DEEP CLONE delta.`{source_tables}/sales_hist`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE users
  DEEP CLONE delta.`{source_tables}/users_hist`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE events
  DEEP CLONE delta.`{source_tables}/events_hist`
  """)

def load_updates():
  spark.sql(f"""
  CREATE OR REPLACE TABLE users_update
  DEEP CLONE delta.`{source_tables}/users_update`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE events_update
  DEEP CLONE delta.`{source_tables}/events_update`
  """)

def load_events_raw():
  spark.sql(f"""
  CREATE OR REPLACE TABLE events_raw
  DEEP CLONE delta.`{source_tables}/events_raw`
  """)
  
def load_item_lookup():
  spark.sql(f"""
  CREATE OR REPLACE TABLE item_lookup
  DEEP CLONE delta.`{source_tables}/item_lookup`
  """)  

load_historical()
load_updates()
load_events_raw()
load_item_lookup()

