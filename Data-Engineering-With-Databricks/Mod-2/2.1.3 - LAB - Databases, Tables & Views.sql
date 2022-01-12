-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databases, Tables, and Views Lab
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC **In this lab, you will create and explore interactions between various relational entities, including:**
-- MAGIC 
-- MAGIC - Databases
-- MAGIC - Tables (managed and external)
-- MAGIC - Views (views, temp views, and global temp views)
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * [Databases and Tables - Databricks Docs](https://docs.databricks.com/user-guide/tables.html)
-- MAGIC * [Managed and Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)
-- MAGIC * [Creating a Table with the UI](https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui)
-- MAGIC * [Create a Local Table](https://docs.databricks.com/user-guide/tables.html#create-a-local-table)
-- MAGIC * [Saving to Persistent Tables](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Getting Started
-- MAGIC 
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-meta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celsius. The schema for the table:
-- MAGIC 
-- MAGIC |ColumnName  | DataType| Description|
-- MAGIC |------------|---------|------------|
-- MAGIC |NAME        |string   | Station name |
-- MAGIC |STATION     |string   | Unique ID |
-- MAGIC |LATITUDE    |float    | Latitude |
-- MAGIC |LONGITUDE   |float    | Longitude |
-- MAGIC |ELEVATION   |float    | Elevation |
-- MAGIC |DATE        |date     | YYYY-MM-DD |
-- MAGIC |UNIT        |string   | Temperature units |
-- MAGIC |TAVG        |float    | Average temperature |
-- MAGIC 
-- MAGIC This data is stored in the Parquet format; preview the data with the query below.

-- COMMAND ----------

SELECT * 
FROM parquet.`${c.userhome}/datasets/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Database
-- MAGIC 
-- MAGIC Create a database in the default location using the `{c.database}` variable defined in setup script.

-- COMMAND ----------

-- TODO

<FILL-IN> ${c.database}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Change to Your New Database
-- MAGIC 
-- MAGIC `USE` your newly created database.

-- COMMAND ----------

-- TODO

<FILL-IN> ${c.database}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Managed Table
-- MAGIC Use a CTAS statement to create a managed table named `weather_managed`.

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${c.userhome}/datasets/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create an External Table
-- MAGIC 
-- MAGIC Recall that an external table differs from a managed table through specification of a location. Create an external table called `weather_external` below.

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${c.userhome}/lab/external"
AS SELECT * 
FROM parquet.`${c.userhome}/datasets/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Examine Table Details
-- MAGIC Use the SQL command `DESCRIBE EXTENDED table_name` to examine the two weather tables.

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following helper code to extract and compare the table locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE EXTENDED {tableName}").select("data_type").filter("col_name = 'Location'").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC 
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC 
-- MAGIC     {managedTablePath}
-- MAGIC 
-- MAGIC The weather_external table is saved at:
-- MAGIC 
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List the contents of these directories to confirm that data exists in both locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(managedTablePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(externalTablePath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check Directory Contents after Dropping Database and All Tables
-- MAGIC The `CASCADE` keyword will accomplish this.
-- MAGIC 
-- MAGIC **NOTE**: You will encounter an error when listing your `managedTablePath`

-- COMMAND ----------

-- TODO

<FILL_IN> ${c.database}

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(managedTablePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(externalTablePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(userhome)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
-- MAGIC 
-- MAGIC Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Database with a Specified Path
-- MAGIC 
-- MAGIC Assuming you dropped your database in the last step, you can use the same `database` name.

-- COMMAND ----------

CREATE DATABASE ${c.database} LOCATION '${c.userhome}/${c.database}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Recreate your `weather_managed` table in this new database and print out the location of this table.

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While here we're using the `userhome` directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views and their Scoping
-- MAGIC 
-- MAGIC Using the provided `AS` clause, register:
-- MAGIC - a view named `celsius`
-- MAGIC - a temporary view named `celsius_temp`
-- MAGIC - a global temp view named `celsius_global`

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now create a temporary view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now register a global temp view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note the following:
-- MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
-- MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
-- MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the `global_temp` database**. The `global_temp` database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While no job was triggered when defining these views, a job is triggered _each time_ a query is executed against the view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean Up
-- MAGIC Drop the database and all tables to clean up your workspace.

-- COMMAND ----------

DROP DATABASE ${c.database} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Synopsis
-- MAGIC 
-- MAGIC In this lab we:
-- MAGIC - Created and deleted databases
-- MAGIC - Explored behavior of managed and external tables
-- MAGIC - Learned about the scoping of views

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
