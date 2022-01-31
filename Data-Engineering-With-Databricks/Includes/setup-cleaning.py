# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

# lesson: Writing delta 
def create_users_update():
    spark.sql(f"""
        CREATE OR REPLACE TABLE users_dirty AS
        SELECT *, current_timestamp() updated 
        FROM parquet.`{source}/users-30m`
    """)
    
    spark.sql("INSERT INTO users_dirty VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)")
  
create_users_update()

