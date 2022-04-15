# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This notebook explores the execution results of a DLT pipeline.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The **`system`** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC  
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

