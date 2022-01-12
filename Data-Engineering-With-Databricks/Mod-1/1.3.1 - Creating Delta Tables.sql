-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Tables

-- COMMAND ----------

-- MAGIC %md The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ../Includes/classic-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC There's not much code you need to write to create a table with Delta.
-- MAGIC 
-- MAGIC We need: 
-- MAGIC - A `CREATE` statement
-- MAGIC - A table name (below we use `test`)
-- MAGIC - A schema (below we use the name `firstcol` and give it a datatype of `int`)
-- MAGIC - The statement USING DELTA\**
-- MAGIC 
-- MAGIC \** In Databricks Runtime 8.0+ the `USING` clause is [optional](https://docs.microsoft.com/en-us/azure/databricks/release-notes/runtime/8.0-migration#:~:text=Databricks%20Runtime%208.0%20changes%20the,or%20%7BDataset%7CDataFrame%7D.&text=While%20Databricks%20recommends%20using%20Delta,require%20migration%20to%20Delta%20Lake.). If you don’t specify the USING clause, DELTA is the default format.

-- COMMAND ----------

CREATE TABLE test (firstcol int);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now go back and run that cell again...it will error out! This is expected - because the table exists already, we receive an error.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC We can add in an additional argument, `IF NOT EXISTS` which checks if the table exists. This will overcome our error.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS test (firstcol int)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `IF NOT EXISTS` checks for the existence of our `test` table. 
-- MAGIC 
-- MAGIC Now that we've seen some basics, let's try reading a Delta table from a source. Many times, we will want to ingest data from a filepath. 
-- MAGIC 
-- MAGIC We're going to ingest some data from a CSV file.

-- COMMAND ----------

CREATE TABLE student (id INT, name STRING, age INT) USING CSV;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC How about another ingest source. What if we have a table already created in our database? We can read the table into Delta.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS preds_staging (
  id STRING,
  batch_date STRING,
  price DOUBLE,
  predicted DOUBLE) 
USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Recall that there are two ways to create tables: from the metastore and not. Assuming a table is in the metastore, you can specify a location and Delta inherits the schema.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS events
  USING DELTA
  LOCATION "/mnt/delta/events";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once that table is created, we can view its contents

-- COMMAND ----------

SELECT * FROM events; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No results?! Looks like we'll need to add some records into the table. Open up the inserts notebook to learn more.

-- COMMAND ----------

-- MAGIC %md What if the table wasn't registered in the metastore already? Let's take a directory of Parquet files and read into Delta.
-- MAGIC 
-- MAGIC Note: we're simplifying here for demonstration purposes. We'd first want to mount a data store like S3 or ADLS to a directory called `intro-data`.

-- COMMAND ----------

CREATE TABLE customers USING PARQUET OPTIONS (path '/intro-data/');
CONVERT TO DELTA customers

-- COMMAND ----------

-- MAGIC %md What about another common use case: create table as (CTAS) statements? Let's create a table and insert some values first:

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), street_address VARCHAR(64), student_id INT)
USING DELTA 

-- COMMAND ----------

INSERT INTO students VALUES
    ('Issac Newton', 'Main Ave', 3145),
    ('Ada Lovelace', 'Not Main Ave', 2718);

-- COMMAND ----------

-- MAGIC %md Now, we could create a new table:

-- COMMAND ----------

CREATE TABLE main_street AS 
  SELECT * FROM students
  WHERE street_address = 'Main Ave'

-- COMMAND ----------

SELECT * 
FROM main_street

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bonus! 
-- MAGIC Check out what's inside of a Delta Transaction log. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(dbutils.fs.head(f"dbfs:/user/hive/warehouse/{database}.db/main_street/_delta_log/00000000000000000000.json"))

-- COMMAND ----------

-- MAGIC %md Let's clean up the tables we created!

-- COMMAND ----------

DROP TABLE test;
DROP TABLE students;
DROP TABLE main_street;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
