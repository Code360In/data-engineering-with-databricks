# Databricks notebook source
# MAGIC %run ./setup-updates

# COMMAND ----------

def merge_deduped_users():
  spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW deduped_users AS
  SELECT user_id, user_first_touch_timestamp, max(email) email, max(updated) updated
  FROM users_update
  GROUP BY user_id, user_first_touch_timestamp
  """)
  
  spark.sql(f"""
  MERGE INTO users a
  USING deduped_users b
  ON a.user_id = b.user_id
  WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
    UPDATE SET email = b.email, updated = b.updated
  WHEN NOT MATCHED THEN INSERT *
  """)  
  

# COMMAND ----------

def merge_events_update():
  spark.sql(f"""
  MERGE INTO events a
  USING events_update b
  ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
  WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
    INSERT *  
  """)
  

# COMMAND ----------

def merge_sales_update():
  spark.sql(f"""
  COPY INTO sales
  FROM "{Paths.source}/sales/sales-30m.parquet"
  FILEFORMAT = PARQUET
  """)
  

# COMMAND ----------

merge_deduped_users()
merge_events_update()
merge_sales_update()

