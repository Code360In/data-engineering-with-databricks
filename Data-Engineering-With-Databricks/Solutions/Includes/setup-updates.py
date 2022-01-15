# Databricks notebook source
# MAGIC %run ./setup-load

# COMMAND ----------

def load_events_raw():
  spark.sql(f"""
  CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset INT, partition BIGINT, timestamp BIGINT, topic STRING, value BINARY)
  USING JSON OPTIONS (path = "{Paths.source}/events/events-kafka.json");
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition BIGINT, timestamp BIGINT, topic STRING, value BINARY);
  """)
  
  spark.sql(f"""
  INSERT INTO events_raw
  SELECT * FROM events_json
  """)  
  

# COMMAND ----------

# lesson: nested data & advanced transformations
# Last Lab & Writing to Delta
def create_events_update():
  spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW events_raw_json AS 
  SELECT from_json(cast(value as STRING), ("device STRING, ecommerce STRUCT< purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT< city: STRING, state: STRING>, items ARRAY< STRUCT< coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING")) json
  FROM events_raw
  """)
 
  spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW events_update AS 
  WITH deduped_events_raw AS (
    SELECT max(json) json FROM events_raw_json
    GROUP BY json.user_id, json.event_timestamp
  )
  SELECT json.* FROM deduped_events_raw
  """) 
  

# COMMAND ----------

# lesson: Writing delta 
def create_users_update():
  spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW users_update AS
  SELECT *, current_timestamp() updated 
  FROM parquet.`{Paths.source}/users/users-30m.parquet`
  """)
  

# COMMAND ----------

load_events_raw()
create_events_update()
create_users_update()


