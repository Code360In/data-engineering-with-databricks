# Databricks notebook source
# MAGIC %run ./setup-clean

# COMMAND ----------

def cart_history():
  spark.sql(f"""
    WITH events_exploded AS (SELECT *,  explode(items) FROM events_clean)
      SELECT user_id, flatten(collect_set(items.item_id)) AS cart_items
      FROM events_exploded GROUP BY user_id;
  """).createOrReplaceTempView("carts")

# COMMAND ----------

if mode != "clean":
  cart_history()


