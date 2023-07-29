# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # End-to-End ETL in the Lakehouse
# MAGIC ## Final Steps
# MAGIC 
# MAGIC We are picking up from the first notebook in this lab, [DE 12.2.1L - Instructions and Configuration]($./DE 12.2.1L - Instructions and Configuration)
# MAGIC 
# MAGIC If everything is setup correctly, you should have:
# MAGIC * A DLT Pipeline running in **Continuous** mode
# MAGIC * A job that is feeding that pipline new data every 2 minutes
# MAGIC * A series of Databricks SQL Queries analysing the outputs of that pipeline

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Execute a Query to Repair Broken Data
# MAGIC 
# MAGIC Review the code that defined the **`recordings_enriched`** table to identify the filter applied for the quality check.
# MAGIC 
# MAGIC In the cell below, write a query that returns all the records from the **`recordings_bronze`** table that were refused by this quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC For the purposes of our demo, let's assume that thorough manual review of our data and systems has demonstrated that occasionally otherwise valid heartrate recordings are returned as negative values.
# MAGIC 
# MAGIC Run the following query to examine these same rows with the negative sign removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC To complete our dataset, we wish to insert these fixed records into the silver **`recordings_enriched`** table.
# MAGIC 
# MAGIC Use the cell below to update the query used in the DLT pipeline to execute this repair.
# MAGIC 
# MAGIC **NOTE**: Make sure you update the code to only process those records that were previously rejected due to the quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC MERGE INTO ${da.db_name}.recordings_enriched t
# MAGIC USING (SELECT
# MAGIC   CAST(a.device_id AS INTEGER) device_id, 
# MAGIC   CAST(a.mrn AS LONG) mrn, 
# MAGIC   abs(CAST(a.heartrate AS DOUBLE)) heartrate, 
# MAGIC   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
# MAGIC   b.name
# MAGIC   FROM ${da.db_name}.recordings_bronze a
# MAGIC   INNER JOIN ${da.db_name}.pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate <= 0) v
# MAGIC ON t.mrn=v.mrn AND t.time=v.time
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Use the cell below to manually or programmatically confirm that this update has been successful.
# MAGIC 
# MAGIC (The total number of records in the **`recordings_bronze`** should now be equal to the total records in **`recordings_enriched`**).

# COMMAND ----------

# ANSWER
assert spark.table(f"{DA.db_name}.recordings_bronze").count() == spark.table(f"{DA.db_name}.recordings_enriched").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Consider Production Data Permissions
# MAGIC 
# MAGIC Note that while our manual repair of the data was successful, as the owner of these datasets, by default we have permissions to modify or delete these data from any location we're executing code.
# MAGIC 
# MAGIC To put this another way: our current permissions would allow us to change or drop our production tables permanently if an errant SQL query is accidentally executed with the current user's permissions (or if other users are granted similar permissions).
# MAGIC 
# MAGIC While for the purposes of this lab, we desired to have full permissions on our data, as we move code from development to production, it is safer to leverage <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">service principals</a> when scheduling Jobs and DLT Pipelines to avoid accidental data modifications.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Shut Down Production Infrastructure
# MAGIC 
# MAGIC Note that Databricks Jobs, DLT Pipelines, and scheduled DBSQL queries and dashboards are all designed to provide sustained execution of production code. In this end-to-end demo, you were instructed to configure a Job and Pipeline for continuous data processing. To prevent these workloads from continuing to execute, you should **Pause** your Databricks Job and **Stop** your DLT pipeline. Deleting these assets will also ensure that production infrastructure is terminated.
# MAGIC 
# MAGIC **NOTE**: All instructions for DBSQL asset scheduling in previous lessons instructed users to set the update schedule to end tomorrow. You may choose to go back and also cancel these updates to prevent DBSQL endpoints from staying on until that time.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
