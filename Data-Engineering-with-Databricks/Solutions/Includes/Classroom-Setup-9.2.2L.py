# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="jobs_lab_92"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

# Don't reset our database or other assets
# DA.cleanup()
DA.init(create_db=False)
DA.data_factory = DltDataFactory()
DA.conclude_setup()

