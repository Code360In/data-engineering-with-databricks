# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Auto Loader and Structured Streaming with Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Ingest data using Auto Loader
# MAGIC * Aggregate streaming data
# MAGIC * Stream data to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ../Includes/classroom-setup-6.3L-classic-setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure Streaming Read
# MAGIC 
# MAGIC This lab uses a collection of customer-related CSV data from DBFS found in */databricks-datasets/retail-org/customers/*.
# MAGIC 
# MAGIC Read this data using Auto Loader using its schema inference (use **`customersCheckpointPath`** to store the schema info). Create a streaming temporary view called **`customers_raw_temp`**.

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

(spark
  .readStream
  <FILL-IN>
  .load("/databricks-datasets/retail-org/customers/")
  .createOrReplaceTempView("customers_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Define a streaming aggregation
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_by_state_temp`** that counts the number of customers per **`state`**, in a field called **`customer_count`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write aggregated data to a Delta table
# MAGIC 
# MAGIC Stream data from the **`customer_count_by_state_temp`** view to a Delta table called **`customer_count_by_state`**.

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"

query = (spark
  <FILL-IN>

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Run the following cell to remove the database and all data associated with this lab.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC By completing this lab, you should now feel comfortable:
# MAGIC * Using PySpark to configure Auto Loader for incremental data ingestion
# MAGIC * Using Spark SQL to aggregate streaming data
# MAGIC * Streaming data to a Delta table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
