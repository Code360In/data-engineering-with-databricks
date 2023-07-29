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
-- MAGIC # Views and CTEs on Databricks
-- MAGIC In this demonstration, you will create and explore views and common table expressions (CTEs).
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define views
-- MAGIC * Run queries that use common table expressions
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">Create View - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">Common Table Expressions - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Classroom Setup
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.2A

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We start by creating a table of data we can use for the demonstration.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flight_delays',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC To show a list of tables (and views), we use the **`SHOW TABLES`** command also demonstrated below.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Views, Temp Views & Global Temp Views
-- MAGIC 
-- MAGIC To set this demonstration up, we are going to first create one of each type of view.
-- MAGIC 
-- MAGIC Then in the next notebook, we will explore the differences between how each one behaves.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### Views
-- MAGIC Let's create a view that contains only the data where the origin is "ABQ" and the destination is "LAX".

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC  
-- MAGIC Note that the **`view_delays_abq_lax`** view has been added to the list below:

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ### Temporary Views
-- MAGIC 
-- MAGIC Next we'll create a temporary view. 
-- MAGIC 
-- MAGIC The syntax is very similar but adds **`TEMPORARY`** to the command.

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Now if we show our tables again, we will see the one table and both views.
-- MAGIC 
-- MAGIC Make note of the values in the **`isTemporary`** column.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### Global Temp Views
-- MAGIC 
-- MAGIC Lastly, we'll create a global temp view. 
-- MAGIC 
-- MAGIC Here we simply add **`GLOBAL`** to the command. 
-- MAGIC 
-- MAGIC Also note the **`global_temp`** database qualifer in the subsequent **`SELECT`** statement.

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Before we move on, review one last time the database's tables and views...

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ...and the tables and views in the **`global_temp`** database:

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Next we are going to demonstrate how tables and views are persisted across multiple sessions and how temp views are not.
-- MAGIC 
-- MAGIC To do this simply open the next notebook, [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont), and continue with the lesson.
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note: There are several scenarios in which a new session may be created:
-- MAGIC * Restarting a cluster
-- MAGIC * Detaching and reataching to a cluster
-- MAGIC * Installing a python package which in turn restarts the Python interpreter
-- MAGIC * Or simply opening a new notebook

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
