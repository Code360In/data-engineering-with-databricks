# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="3.1"

# COMMAND ----------

DA.cleanup()
DA.init(create_db=False)
install_dtavod_datasets(reinstall=False)
print()
copy_source_dataset(f"{DA.working_dir_prefix}/source/dtavod/flights/departuredelays.csv", f"{DA.paths.working_dir}/flights/departuredelays.csv", "csv", "flights")
DA.conclude_setup()

