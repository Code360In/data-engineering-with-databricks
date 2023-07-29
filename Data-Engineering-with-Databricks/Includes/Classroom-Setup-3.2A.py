# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="3.2"

# COMMAND ----------

DA.cleanup()
DA.init()

# Clean out the global_temp database.
for row in spark.sql("SHOW TABLES IN global_temp").select("tableName").collect():
    table_name = row[0]
    spark.sql(f"DROP TABLE global_temp.{table_name}")

print()
install_dtavod_datasets(reinstall=False)
DA.conclude_setup()

