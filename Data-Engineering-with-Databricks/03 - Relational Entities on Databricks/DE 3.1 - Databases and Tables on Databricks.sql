-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Databases and Tables on Databricks
-- MAGIC In this demonstration, you will create and explore databases and tables.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define databases and tables
-- MAGIC * Describe how the **`LOCATION`** keyword impacts the default storage directory
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databases and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Lesson Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Using Hive Variables
-- MAGIC 
-- MAGIC While not a pattern that is generally recommended in Spark SQL, this notebook will use some Hive variables to substitute in string values derived from the account email of the current user.
-- MAGIC 
-- MAGIC The following cell demonstrates this pattern.

-- COMMAND ----------

SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Because you may be working in a shared workspace, this course uses variables derived from your username so the databases don't conflict with other users. Again, consider this use of Hive variables a hack for our lesson environment rather than a good practice for development.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Databases
-- MAGIC Let's start by creating two databases:
-- MAGIC - One with no **`LOCATION`** specified
-- MAGIC - One with **`LOCATION`** specified

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Note that the location of the first database is in the default location under **`dbfs:/user/hive/warehouse/`** and that the database directory is the name of the database with the **`.db`** extension

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that the location of the second database is in the directory specified after the **`LOCATION`** keyword.

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We will create a table in the database with default location and insert data. 
-- MAGIC 
-- MAGIC Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We can look at the extended table description to find the location (you'll need to scroll down in the results).

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC By default, managed tables in a database without the location specified will be created in the **`dbfs:/user/hive/warehouse/<database_name>.db/`** directory.
-- MAGIC 
-- MAGIC We can see that, as expected, the data and metadata for our Delta Table are stored in that location.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hive_root =  f"dbfs:/user/hive/warehouse"
-- MAGIC db_name =    f"{DA.db_name}_default_location.db"
-- MAGIC table_name = f"managed_table_in_db_with_default_location"
-- MAGIC 
-- MAGIC tbl_location = f"{hive_root}/{db_name}/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Note the table's directory and its log and data files are deleted. Only the database directory remains.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location = f"{hive_root}/{db_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We now create a table in  the database with custom location and insert data. 
-- MAGIC 
-- MAGIC Note that the schema must be provided because there is no data from which to infer the schema.

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Again, we'll look at the description to find the table location.

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC As expected, this managed table is created in the path specified with the **`LOCATION`** keyword during database creation. As such, the data and metadata for the table are persisted in a directory here.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC table_name = f"managed_table_in_db_with_custom_location"
-- MAGIC tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Let's drop the table.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Note the table's folder and the log file and data file are deleted.  
-- MAGIC   
-- MAGIC Only the database location remains

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC 
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Tables
-- MAGIC We will create an external (unmanaged) table from sample data. 
-- MAGIC 
-- MAGIC The data we are going to use are in CSV format. We want to create a Delta table with a **`LOCATION`** provided in the directory of our choice.

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Let's note the location of the table's data in this lesson's working directory.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Now, we drop the table.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC The table definition no longer exists in the metastore, but the underlying data remain intact.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Clean up
-- MAGIC Drop both databases.

-- COMMAND ----------

DROP DATABASE ${da.db_name}_default_location CASCADE;
DROP DATABASE ${da.db_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
