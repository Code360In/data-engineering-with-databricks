# Databricks notebook source
# MAGIC %run ../../Includes/classroom-setup-dlt-lab

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This Notebook explores the execution results of a DLT pipeline. Before proceeding, you will need one piece of information specific to your pipeline instance: the location in DBFS where results are stored. Because we did not specify a value for **Storage Location** when creating the pipeline, DLT automatically created a folder for us. Obtain this information as follows.
# MAGIC 
# MAGIC Click **Settings** on the **Pipeline Details** page. This provides a JSON representation of the pipeline configuration. Copy the value specified for **storage** and substitute for **<storage>** throughout the rest of this Notebook. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> Generally, and particularly in production systems, you will specify **Storage Location** in your pipeline configurations to have full control of where pipeline results are stored.

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

