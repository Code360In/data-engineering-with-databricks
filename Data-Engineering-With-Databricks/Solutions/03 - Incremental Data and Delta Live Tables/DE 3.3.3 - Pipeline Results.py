# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This Notebook explores the execution results of a DLT pipeline.

# COMMAND ----------

# MAGIC %run ../Includes/dlt-setup $course="dlt_demo"

# COMMAND ----------

storage_location = userhome

# COMMAND ----------

dbutils.fs.ls(storage_location)

# COMMAND ----------

# MAGIC %md
# MAGIC The `system` directory captures events associated with the pipeline.

# COMMAND ----------

dbutils.fs.ls(f"{storage_location}/system/events")

# COMMAND ----------

# MAGIC %md
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{storage_location}/system/events`"))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

dbutils.fs.ls(f"{storage_location}/tables")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's query the gold table.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {database}.sales_order_in_la"))

