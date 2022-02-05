# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="2.2.4"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.cleanup()
DA.init()
install_eltwss_datasets()

print()

clone_source_table("sales", f"{DA.paths.datasets}/delta", "sales_hist")
clone_source_table("users", f"{DA.paths.datasets}/delta", "users_hist")
clone_source_table("events", f"{DA.paths.datasets}/delta", "events_hist")

clone_source_table("users_update", f"{DA.paths.datasets}/delta")
clone_source_table("events_update", f"{DA.paths.datasets}/delta")

# clone_source_table("events_raw", f"{DA.paths.datasets}/delta")
# clone_source_table("item_lookup", f"{DA.paths.datasets}/delta")
    
DA.conclude_setup()

