# Databricks notebook source
# MAGIC %run ./sql-setup $course="meta" $mode="cleanup"

# COMMAND ----------

URI = "wasbs://courseware@dbacademy.blob.core.windows.net/databases_tables_and_views_on_databricks/v02"

# COMMAND ----------

dbutils.fs.cp(URI, f"{userhome}/datasets", True)

