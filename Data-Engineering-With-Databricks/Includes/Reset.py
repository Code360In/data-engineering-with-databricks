# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="reset"

# COMMAND ----------

# MAGIC %run ./_utility-methods

# COMMAND ----------

DA.init()

# COMMAND ----------

rows = spark.sql(f"show databases").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(DA.db_name_prefix):
        print(db_name)
        spark.sql(f"DROP DATABASE {db_name} CASCADE")

# COMMAND ----------

if DA.paths.exists(DA.working_dir_prefix):
    print(DA.working_dir_prefix)
    dbutils.fs.rm(DA.working_dir_prefix, True)

# COMMAND ----------

# Create the source database, install the datasets, whatever, so that we can run the other notebooks asyncronously
install_dtavod_datasets()
install_eltwss_datasets()

# COMMAND ----------

# MAGIC %run ./mount-datasets
