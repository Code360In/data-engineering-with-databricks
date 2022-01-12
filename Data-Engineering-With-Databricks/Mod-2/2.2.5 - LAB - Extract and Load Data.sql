-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extract and Load Data Lab
-- MAGIC 
-- MAGIC Load raw events data from JSON files into a Delta table. Load products data from parquet files into a Delta table.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Extract data from JSON files
-- MAGIC - Create empty Delta table with schema
-- MAGIC - Append extracted data to Delta table
-- MAGIC - Create Delta table by querying files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-complex

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Extract Raw Events From JSON Files
-- MAGIC Extract data by creating an external JSON table against the files.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS events_json
(key BINARY, offset INT, partition BIGINT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON OPTIONS (path = "${c.source}/events/events-kafka.json");

-- COMMAND ----------

-- MAGIC %md ## Insert Raw Events Into Delta Table
-- MAGIC - Create an empty Delta Table with the correct schema for raw events.
-- MAGIC - Insert raw event records from the external JSON table to the Delta table.

-- COMMAND ----------

CREATE OR REPLACE TABLE events_raw
(key BINARY, offset BIGINT, partition BIGINT, timestamp BIGINT, topic STRING, value BINARY);

INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- MAGIC %md ## Create Delta Table from a Query
-- MAGIC 
-- MAGIC Create Delta Table by Querying Files

-- COMMAND ----------

CREATE OR REPLACE TABLE item_lookup AS
SELECT * FROM parquet.`${c.source}/products/products.parquet`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
