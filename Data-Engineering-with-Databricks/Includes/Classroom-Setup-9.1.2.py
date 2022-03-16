# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="jobs_demo_91"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

DA.cleanup()
DA.init()
DA.data_factory = DltDataFactory()
DA.conclude_setup()

