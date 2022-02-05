# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.2.6"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets()

print()
create_eltwss_users_update()
    
DA.conclude_setup()

