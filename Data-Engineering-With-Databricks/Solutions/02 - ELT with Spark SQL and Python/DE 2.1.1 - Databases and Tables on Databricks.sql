-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databases and Tables on Databricks
-- MAGIC In this demonstration, you will create and explore databases and tables.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC * Use Spark SQL DDL to define databases and tables
-- MAGIC * Describe how the `LOCATION` keyword impacts the default storage directory
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
-- MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
-- MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
-- MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
-- MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lesson Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-meta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using Hive Variables
-- MAGIC 
-- MAGIC While not a pattern that is generally recommended in Spark SQL, this notebook will use some Hive variables to substitute in string values derived from the account email of the current user.
-- MAGIC 
-- MAGIC The following cell demonstrates this pattern.

-- COMMAND ----------

SELECT "${c.database}";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Because you may be working in a shared workspace, this course uses variables derived from your username so the databases don't conflict with other users. Again, consider this use of Hive variables a hack for our lesson environment rather than a good practice for development.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Databases
-- MAGIC Let's start by creating two databases:
-- MAGIC - One with no LOCATION specified
-- MAGIC - One with LOCATION specified 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${c.database}_default_location;
CREATE DATABASE IF NOT EXISTS ${c.database}_custom_location LOCATION '${c.userhome}';

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Note that the location of the first database is in the default location under `dbfs:/user/hive/warehouse/`

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${c.database}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that the location of the second database is in the directory specified after the `LOCATION` keyword.

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${c.database}_custom_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC We will create a table in the database with default location and insert data. Note that the schema must be provided because there are no data from which to infer the schema.

-- COMMAND ----------

USE ${c.database}_default_location;
CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By default, managed tables in a database without the location specified will be created in the `dbfs:/user/hive/warehouse/<database_name>.db/` directory.
-- MAGIC 
-- MAGIC We can see that, as expected, the data and metadata for our Delta Table are stored in that location.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database}_default_location.db/managed_table_in_db_with_default_location"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Note the table's folder and its log and data file are deleted.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database}_default_location.db")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC We now create a table in  the database with custom location and insert data. Note that the schema must be provided because there are no data from which to infer the schema.

-- COMMAND ----------

USE ${c.database}_custom_location;
CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Again, we'll look at the description to find the table location.

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC As expected, this managed table is created in the path specified with the `LOCATION` keyword during database creation. As such, the data and metadata for the table are persisted in a directory here.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"{userhome}/managed_table_in_db_with_custom_location"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Let's drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Note the table's folder and the log file and data file are deleted.  
-- MAGIC   
-- MAGIC Only the "datasets" folder remains.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls(userhome)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Tables
-- MAGIC We will create an external (unmanaged) table from sample data. The data we are going to use are in csv format. We want to create a Delta table with a LOCATION provided in the directory of our choice.

-- COMMAND ----------

USE ${c.database}_default_location;

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path '${c.userhome}/datasets/flights/departuredelays.csv',
  header "true",
  mode "FAILFAST"
);
CREATE OR REPLACE TABLE external_table LOCATION '${c.userhome}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"{userhome}/external_table"))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Clean up
-- MAGIC Drop both databases.

-- COMMAND ----------

DROP DATABASE ${c.database}_default_location CASCADE;
DROP DATABASE ${c.database}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delete the working directory and its contents.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.rm(userhome, True)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
