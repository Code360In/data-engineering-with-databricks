-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Providing Options for External Sources
-- MAGIC While directly querying files works well for self-describing formats, many data sources require additional configurations or schema declaration to properly ingest records.
-- MAGIC 
-- MAGIC In this lesson, we will create tables using external data sources. While these tables will not yet be stored in the Delta Lake format (and therefore not be optimized for the Lakehouse), this technique helps to facilitate extracting data from diverse external systems.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Use Spark SQL to configure options for extracting data from external sources
-- MAGIC - Create tables against external data sources for various file formats
-- MAGIC - Describe default behavior when querying tables defined against external sources

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## When Direct Queries Don't Work 
-- MAGIC 
-- MAGIC While views can be used to persist direct queries against files between sessions, this approach has limited utility.
-- MAGIC 
-- MAGIC CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.

-- COMMAND ----------

SELECT * FROM csv.`${c.source}/sales/sales.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can see from the above that:
-- MAGIC 1. The header row is being extracted as a table row
-- MAGIC 1. All columns are being loaded as a single column
-- MAGIC 1. The file is pipe-delimited (`|`)
-- MAGIC 1. The final column appears to contain nested data that is being truncated

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Registering Tables on External Data with Read Options
-- MAGIC 
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC 
-- MAGIC While there are many [additional configurations](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-datasource.html) you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.
-- MAGIC 
-- MAGIC ```
-- MAGIC CREATE TABLE table_identifier
-- MAGIC (col_name1 col_type1, ...)
-- MAGIC USING data_source
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)
-- MAGIC LOCATION path
-- MAGIC ```
-- MAGIC 
-- MAGIC Note that options are passed with keys as unquoted text and values in quotes. Spark supports many [data sources](https://docs.databricks.com/data/data-sources/index.html) with custom options, and additional systems may have unofficial support through external [libraries](https://docs.databricks.com/libraries/index.html). 
-- MAGIC 
-- MAGIC **NOTE**: Depending on your workspace settings, you may need adminstrator assistance to load libraries and configure the requisite security settings for some data sources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:
-- MAGIC 1. The column names and types
-- MAGIC 1. The file format
-- MAGIC 1. The delimiter used to separate fields
-- MAGIC 1. The presence of a header
-- MAGIC 1. The path to where this data is stored

-- COMMAND ----------

DROP TABLE IF EXISTS sales_csv;

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${c.source}/sales/sales.csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that no data has moved during table declaration. Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location.
-- MAGIC 
-- MAGIC Run the following cell to confirm that data is now being loaded correctly.

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.
-- MAGIC 
-- MAGIC **NOTE**: When working with CSVs as a data source, it's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.
-- MAGIC 
-- MAGIC Running `DESCRIBE EXTENDED` on a table will show all of the metadata associated with the table definition.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limits of Tables with External Data Sources
-- MAGIC 
-- MAGIC If you've taken other courses on Databricks or reviewed any of our company literature, you may have heard about Delta Lake and the Lakehouse. Note that whenever we're defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and Lakehouse.
-- MAGIC 
-- MAGIC For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.
-- MAGIC 
-- MAGIC The cell below executes some logic that we can think of as just representing an external system directly updating the files underlying our table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("sales_csv").write.mode("append").format("csv").save(f"{Paths.source}/sales/sales.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we look at the current count of records in our table, the number we see will not reflect these newly inserted rows.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC At the time we previously queried this data source, Spark automatically cached the underlying data in local storage. This ensures that on subsequent queries, Spark will provide the optimal performance by just querying this local cache.
-- MAGIC 
-- MAGIC Our external data source is not configured to tell Spark is should refresh this data. We **can** manually refresh the cache of our data by running the `REFRESH TABLE` command.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that refreshing our table will invalidate our cache, meaning that we'll need to rescan our original data source and pull all data back into memory. For very large datasets, this may take a significant amount of time.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extracting Data from SQL Databases
-- MAGIC SQL databases are an extremely common data source, and Databricks has a standard JDBC driver for connecting with many flavors of SQL.
-- MAGIC 
-- MAGIC The general syntax for creating these connections is:
-- MAGIC ```
-- MAGIC CREATE TABLE <jdbcTable>
-- MAGIC USING org.apache.spark.sql.jdbc
-- MAGIC OPTIONS (
-- MAGIC   url "jdbc:<databaseServerType>://<jdbcHostname>:<jdbcPort>",
-- MAGIC   dbtable "<jdbcDatabase>.table",
-- MAGIC   user "<jdbcUsername>",
-- MAGIC   password "<jdbcPassword>"
-- MAGIC )
-- MAGIC ```
-- MAGIC 
-- MAGIC In the code sample below, we'll connect with SQLite (which uses a local file to store a database, and doesn't have users and passwords).

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlite:/${c.username}_ecommerce.db",
  dbtable "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we can query this table as if it were defined locally.

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Looking at the table metadata reveals that we have captured the schema information from the external system. Storage properties (which would include the username and password associated with the connection) are automatically redacted.

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While the table is listed as `MANAGED`, listing the contents of the specified location confirms that no data is being persisted locally.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbc_users_path = f"{Paths.database_location}/jdbc_users"
-- MAGIC print(jdbc_users_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(jdbc_users_path)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that some SQL systems such as data warehouses will have custom drivers. Spark will interact with various external databases differently, but the two basic approaches can be summarized as either:
-- MAGIC 1. Moving the entire source table(s) to Databricks and then executing logic on the currently active cluster
-- MAGIC 1. Pushing down the query to the external SQL database and only transferring the results back to Databricks
-- MAGIC 
-- MAGIC In either case, working with very large datasets in external SQL databases can incur significant overhead because of either:
-- MAGIC 1. Network transfer latency associated with moving all data over the public internet
-- MAGIC 1. Execution of query logic in source systems not optimized for big data queries

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
