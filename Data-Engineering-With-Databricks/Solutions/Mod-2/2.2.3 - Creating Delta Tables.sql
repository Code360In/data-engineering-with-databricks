-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Delta Tables
-- MAGIC 
-- MAGIC After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged.
-- MAGIC 
-- MAGIC While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.
-- MAGIC 
-- MAGIC This lesson will focus primarily on the pattern used to create most tables, `CREATE TABLE _ AS SELECT` (CTAS) statements.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Use CTAS statements to create Delta Lake tables
-- MAGIC - Create new tables from existing views or tables
-- MAGIC - Enrich loaded data with additional metadata
-- MAGIC - Declare table schema with generated columns and descriptive comments
-- MAGIC - Set advanced options to control data location, quality enforcement, and partitioning

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-extract

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create an Empty Delta Table
-- MAGIC Use the `CREATE TABLE USING` statement to define an empty Delta table in the metastore.  
-- MAGIC 
-- MAGIC We won't need to explicitly state `USING DELTA`, as Delta is the default format.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo_users (user_id STRING, email STRING);

DESCRIBE EXTENDED demo_users

-- COMMAND ----------

-- MAGIC %md ## Create Table as Select (CTAS)
-- MAGIC 
-- MAGIC `CREATE TABLE AS SELECT` statements create and populate Delta tables using data retrieved from an input query.  
-- MAGIC 
-- MAGIC Because they inherit schemas from the query data, CTAS statements do **not** support schema declarations. In the following query, the CTAS statement can not provide the options necessary to properly ingest data from CSV files.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${c.source}/sales/sales.csv`;

SELECT * FROM sales_unparsed

-- COMMAND ----------

DESCRIBE EXTENDED sales_unparsed

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Subset Columns from Existing Tables
-- MAGIC We can also use CTAS statements to load data from existing tables and views.
-- MAGIC 
-- MAGIC Let's review the `csv_orders` table we created in the previous lesson, which used a `CREATE TABLE USING` statement to define a read on CSV data with various options specified.

-- COMMAND ----------

SELECT * FROM csv_orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While this data is accessible from Databricks, it won't currently have any of the performance guarantees that Delta Lake provides.
-- MAGIC 
-- MAGIC The following CTAS statement creates a new table containing a subset of columns from this table. Here, we'll presume that we're intentionally leaving out information that potentially identifies the user or that provides itemized purchase details.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id, transactions_timestamp, purchase_revenue_in_usd
FROM csv_orders;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Declare Schema with Generated Columns
-- MAGIC 
-- MAGIC As noted previously, CTAS statements do not support schema declaration. We note above that the timestamp column appears to be some variant of a Unix timestamp, which may not be the most useful for our analysts to derive insights. This is a situation where generated columns would be beneficial.
-- MAGIC 
-- MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table. Databricks introduced generated columns in DBR 8.3.
-- MAGIC 
-- MAGIC The code below demonstrates creating a new table while:
-- MAGIC 1. Specifying column names and types
-- MAGIC 1. Adding a [generated column](https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns) to calculate the date
-- MAGIC 1. Providing a descriptive column comment for the generated column

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_w_date (
  order_id STRING, 
  transactions_timestamp STRING, 
  purchase_revenue_in_usd STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transactions_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Because `date` is a generated column, if we write to `purchases_w_date` without providing values for the `date` column, Delta Lake automatically computes them.
-- MAGIC 
-- MAGIC **NOTE**: The cell below configures a setting to allow for generating columns when using a Delta Lake `MERGE` statement. We'll see more on this syntax later in the course.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchases_w_date a
USING purchases b
ON a.order_id = b.order_id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md The query automatically reads the most recent snapshot of the table for any query; you never need to run `REFRESH TABLE`.

-- COMMAND ----------

SELECT * FROM purchases_w_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enrich Tables with Additional Options and Metadata
-- MAGIC 
-- MAGIC So far we've only shown creating managed tables, which will store all data associated with a newly created table under the databases default location. For many use cases, these will be the right choice.
-- MAGIC 
-- MAGIC Here, we'll create an external Delta Lake table and provide a number of additional options. The syntax below will:
-- MAGIC 1. Add a table comment
-- MAGIC 1. Add an arbitrary key-value pair as a table property
-- MAGIC 1. Add a date column derived from source table data
-- MAGIC 1. Add a column to record current timestamp at time of update
-- MAGIC 1. Configure table `LOCATION` path
-- MAGIC 1. Partition tables by a column
-- MAGIC 
-- MAGIC **NOTE**: A number of Delta Lake configurations are set using `TBLPROPERTIES`. When using this field as part of an organizational approach to data discovery and auditing, users should be made aware of which keys are leveraged for modifying default Delta Lake behaviors.

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
TBLPROPERTIES ('contains_pii' = True) 
LOCATION "${c.userhome}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated 
  FROM jdbc_users

-- COMMAND ----------

-- MAGIC %md Specifying the `LOCATION` path creates an external Delta table that is unmanaged by the Metastore.
-- MAGIC 
-- MAGIC All of the comments and properties for a given table can be reviewed using `DESCRIBE TABLE EXTENDED`.
-- MAGIC 
-- MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Listing the location used for the table reveals that the unique values in the partition column `date` are used to create data directories.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"{Paths.userhome}/tmp/users_pii"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Delta Lake automatically uses partitioning and statistics to read the minimum amount of data when there are applicable predicates in the query. 

-- COMMAND ----------

SELECT * FROM users_pii WHERE first_touch_date = "2020-06-19"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Add a Table Constraint
-- MAGIC 
-- MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
-- MAGIC 
-- MAGIC Databricks currently support two types of constraints:
-- MAGIC * [`NOT NULL` constraints](https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint)
-- MAGIC * [`CHECK` constraints](https://docs.databricks.com/delta/delta-constraints.html#check-constraint)
-- MAGIC 
-- MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
-- MAGIC 
-- MAGIC Below, we'll add a `CHECK` constraint to the `date` column of our table. Note that `CHECK` constraints look like standard `WHERE` clauses you might use to filter a dataset.

-- COMMAND ----------

ALTER TABLE users_pii ADD CONSTRAINT valid_date CHECK (first_touch_date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table constraints are shown in the `TBLPROPERTIES` field, alongside user-specified tags and other Delta Lake metadata.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
