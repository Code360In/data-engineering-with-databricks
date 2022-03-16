# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="cap_12"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

# Don't clean up, continue where we left off.
# DA.cleanup()
DA.init(create_db=False)
DA.data_factory = DltDataFactory()
DA.conclude_setup()

