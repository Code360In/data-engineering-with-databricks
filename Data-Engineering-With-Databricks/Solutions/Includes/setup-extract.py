# Databricks notebook source
# MAGIC %run ./setup-external

# COMMAND ----------

csv_table_name = "csv_orders"
jdbc_table_name = "jdbc_users"

def parse_csv_orders():
  spark.sql(f"DROP TABLE IF EXISTS {csv_table_name}")

  spark.sql(f"""
  CREATE TABLE {csv_table_name} (
    order_id STRING,
    email STRING,
    transactions_timestamp STRING,
    total_item_quantity INTEGER,
    purchase_revenue_in_usd STRING,
    unique_items STRING,
    items STRING
  ) 
  USING CSV
  OPTIONS (
    header = "true",
    delimiter = "|"
  ) 
  LOCATION "{Paths.source}/sales/sales.csv"
  """)

def connect_jdbc_users():
  spark.sql(f"DROP TABLE IF EXISTS {jdbc_table_name}")

  spark.sql(f"""
  CREATE TABLE {jdbc_table_name} 
  USING org.apache.spark.sql.jdbc
  OPTIONS (
    url "jdbc:sqlite:/{username}_ecommerce.db",
    dbtable "users"
  )
  """)

# COMMAND ----------

if mode != "clean":
  parse_csv_orders()
  connect_jdbc_users()


