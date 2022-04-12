# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.1"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)
DA.conclude_setup()

