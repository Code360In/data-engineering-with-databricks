-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Writing to Delta Tables
-- MAGIC Use SQL DML statements to perform complete and incremental updates to existing Delta tables.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Overwrite data tables using `INSERT OVERWRITE`
-- MAGIC - Append to a table using `INSERT INTO`
-- MAGIC - Append, update, and delete from a table using `MERGE INTO`
-- MAGIC - Ingest data incrementally into tables using `COPY INTO`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-updates

-- COMMAND ----------

-- MAGIC %md ## Complete Overwrites
-- MAGIC 
-- MAGIC We can use overwrites to atomically replace all of the data in a table. There are multiple benefits to overwriting tables instead of deleting and recreating tables:
-- MAGIC - Overwriting a table is much faster because it doesn’t need to list the directory recursively or delete any files.
-- MAGIC - The old version of the table still exists; can easily retrieve the old data using Time Travel.
-- MAGIC - It’s an atomic operation. Concurrent queries can still read the table while you are deleting the table.
-- MAGIC - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC 
-- MAGIC The following cells demonstrate two ways to overwrite data.
-- MAGIC 
-- MAGIC 1. Using the `CREATE OR REPLACE TABLE` statement
-- MAGIC 2. Using the `INSERT OVERWRITE` statement

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${c.source}/events/events.parquet`

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${c.source}/sales/sales.parquet`

-- COMMAND ----------

-- MAGIC %md This keeps history of the previous table, but rewrites all data.

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- MAGIC %md ## Append Rows
-- MAGIC 
-- MAGIC We can use `INSERT INTO` to atomically append new rows to an existing Delta table. This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.
-- MAGIC 
-- MAGIC Append new sale records to the `sales` table using `INSERT INTO`.

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${c.source}/sales/sales-30m.parquet`

-- COMMAND ----------

-- MAGIC %md ## Merge Updates
-- MAGIC 
-- MAGIC You can upsert data from a source table, view, or DataFrame into a target Delta table using the `MERGE` SQL operation. Delta Lake supports inserts, updates and deletes in `MERGE`, and supports extended syntax beyond the SQL standards to facilitate advanced use cases.
-- MAGIC ```
-- MAGIC MERGE INTO target a
-- MAGIC USING source b
-- MAGIC ON <merge_condition>
-- MAGIC WHEN MATCHED THEN <matched_action>
-- MAGIC WHEN NOT MATCHED THEN <not_matched_action>
-- MAGIC ```
-- MAGIC We will use the `MERGE` operation to update historic users data with updated emails and new users.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() updated FROM parquet.`${c.source}/users/users-30m.parquet`

-- COMMAND ----------

-- MAGIC %md As we implemented the `users` table as a Type 1 SCD Delta table with an `updated` field, we can leverage this field while performing a merge operation. Let's make sure that records that are updated OR inserted have the same timestamp. This operation will be completed as a single batch to avoid potentially leaving our table in a corrupt state.

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert-Only Merge for Deduplication
-- MAGIC 
-- MAGIC A common ETL use case is to collect logs into Delta table by appending them to a table. However, often the sources can generate duplicate log records and downstream deduplication steps are needed to take care of them. With merge, you can avoid inserting the duplicate records.
-- MAGIC 
-- MAGIC When merging new events parsed from `events_raw`, confirm that an identical record isn't already in the `events` table.
-- MAGIC ```
-- MAGIC MERGE INTO target a
-- MAGIC USING source b
-- MAGIC ON a.data = b.data
-- MAGIC WHEN NOT MATCHED THEN 
-- MAGIC   INSERT *
-- MAGIC ```
-- MAGIC By default, the merge operation searches the entire Delta table to find matches in the source table. One way to speed up merge is to reduce the search space by adding known constraints in the match condition. It will also reduce the chances of conflicts with other concurrent operations.
-- MAGIC ```
-- MAGIC events.country = 'USA' AND events.date = current_date() - INTERVAL 7 DAYS
-- MAGIC ```  

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- MAGIC %md ## Load Incrementally
-- MAGIC Use `COPY INTO` to incrementally load data from external systems.
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC - Potentially much cheaper than full table scan for data that grows predictably
-- MAGIC - Leveraged by many data ingestion partners
-- MAGIC 
-- MAGIC Update the `sales` delta table by incrementally loading data from an external location where a number of new transactions arrive during a 30 minute window. Each sale should only be recorded once, at the time that the transaction is processed. Use a method that allows idempotent execution to avoid processing data multiple times.

-- COMMAND ----------

COPY INTO sales
FROM "${c.source}/sales/sales-30m.parquet"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
