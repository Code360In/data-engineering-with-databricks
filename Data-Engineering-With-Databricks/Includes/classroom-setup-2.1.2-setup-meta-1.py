# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.1.2"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.cleanup()
DA.init(create_db=False)
print()
install_dtavod_datasets()
DA.conclude_setup()

