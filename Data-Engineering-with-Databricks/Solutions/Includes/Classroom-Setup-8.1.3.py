# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_demo_81"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

# Continues where 8.1.1 picks up, don't remove assets
# DA.cleanup()
DA.init()

DA.paths.data_source = "/mnt/training/healthcare"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"
DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

DA.data_factory = DltDataFactory()
DA.conclude_setup()

