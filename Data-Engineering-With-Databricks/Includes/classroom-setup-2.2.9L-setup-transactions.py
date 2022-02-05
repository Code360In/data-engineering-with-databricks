# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.2.5L"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets()
print()
load_eltwss_tables()
DA.conclude_setup()

