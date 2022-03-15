# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="4.6"

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets(reinstall=False)

print()
create_eltwss_users_update()
    
DA.conclude_setup()

