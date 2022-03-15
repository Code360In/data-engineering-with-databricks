# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.9L"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
print()
load_eltwss_tables()
DA.conclude_setup()

