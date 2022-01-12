# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This Notebook explores the execution results of a DLT pipeline. Before proceeding, you will need one piece of information specific to your pipeline instance: the location in DBFS where results are stored. Because we did not specify a value for **Storage Location** when creating the pipeline, DLT automatically created a folder for us. Obtain this information as follows.
# MAGIC 
# MAGIC Click **Settings** on the **Pipeline Details** page. This provides a JSON representation of the pipeline configuration. Copy the value specified for **storage** and substitute for `<storage>` throughout the rest of this Notebook. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> Generally, and particularly in production systems, you will specify **Storage Location** in your pipeline configurations to have full control of where pipeline results are stored.

# COMMAND ----------

# MAGIC %md
# MAGIC ## View contents of pipeline directory
# MAGIC 
# MAGIC Execute the following cell to view the contents of the pipeline directory. There, you will find subdirectories containing administrative information, logs, and the tables themselves. 

# COMMAND ----------

# MAGIC %fs ls <storage>

# COMMAND ----------

# MAGIC %md
# MAGIC The `system` directory captures events associated with the pipeline.

# COMMAND ----------

# MAGIC %fs ls <storage>/system/events

# COMMAND ----------

# MAGIC %md
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`<storage>/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

# MAGIC %fs ls <storage>/tables

# COMMAND ----------

# MAGIC %md
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`<storage>/tables/sales_order_in_la`

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"/> If **Target** is specified in the pipeline configuration, then tables will be published to the specified database in the metastore from where they could more readily be queried. This would also be a typical production configuration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
