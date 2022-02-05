# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.2.3"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets()
load_eltwss_external_tables()
DA.conclude_setup()

