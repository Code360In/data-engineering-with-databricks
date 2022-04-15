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
-- MAGIC # SQL UDFs and Control Flow
-- MAGIC 
-- MAGIC Databricks added support for User Defined Functions (UDFs) registered natively in SQL starting in DBR 9.1.
-- MAGIC 
-- MAGIC This feature allows users to register custom combinations of SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions leverage Spark SQL directly, maintaining all of the optimizations of Spark when applying your custom logic to large datasets.
-- MAGIC 
-- MAGIC In this notebook, we'll first have a simple introduction to these methods, and then explore how this logic can be combined with **`CASE`** / **`WHEN`** clauses to provide reusable custom control flow logic.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Define and registering SQL UDFs
-- MAGIC * Describe the security model used for sharing SQL UDFs
-- MAGIC * Use **`CASE`** / **`WHEN`** statements in SQL code
-- MAGIC * Leverage **`CASE`** / **`WHEN`** statements in SQL UDFs for custom control flow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC Run the following cell to setup your environment.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create a Simple Dataset
-- MAGIC 
-- MAGIC For this notebook, we'll consider the following dataset, registered here as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW foods(food) AS VALUES
("beef"),
("beans"),
("potatoes"),
("bread");

SELECT * FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## SQL UDFs
-- MAGIC At minimum, a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC 
-- MAGIC Below, a simple function named **`yelling`** takes one parameter named **`text`**. It returns a string that will be in all uppercase letters with three exclamation points added to the end.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that this function is applied to all values of the column in a parallel fashion within the Spark processing engine. SQL UDFs are an efficient way to define custom logic that is optimized for execution on Databricks.

-- COMMAND ----------

SELECT yelling(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Scoping and Permissions of SQL UDFs
-- MAGIC 
-- MAGIC Note that SQL UDFs will persist between execution environments (which can include notebooks, DBSQL queries, and jobs).
-- MAGIC 
-- MAGIC We can describe the function to see where it was registered and basic information about expected inputs and what is returned.

-- COMMAND ----------

DESCRIBE FUNCTION yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC By describing extended, we can get even more information. 
-- MAGIC 
-- MAGIC Note that the **`Body`** field at the bottom of the function description shows the SQL logic used in the function itself.

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC SQL UDFs exist as objects in the metastore and are governed by the same Table ACLs as databases, tables, or views.
-- MAGIC 
-- MAGIC In order to use a SQL UDF, a user must have **`USAGE`** and **`SELECT`** permissions on the function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## CASE/WHEN
-- MAGIC 
-- MAGIC The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
-- MAGIC 
-- MAGIC Again, everything is evaluated natively in Spark, and so is optimized for parallel execution.

-- COMMAND ----------

SELECT *,
  CASE 
    WHEN food = "beans" THEN "I love beans"
    WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
    WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
    ELSE concat("I don't eat ", food)
  END
FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Simple Control Flow Functions
-- MAGIC 
-- MAGIC Combining SQL UDFs with control flow in the form of **`CASE`** / **`WHEN`** clauses provides optimized execution for control flows within SQL workloads.
-- MAGIC 
-- MAGIC Here, we demonstrate wrapping the previous logic in a function that will be reusable anywhere we can execute SQL.

-- COMMAND ----------

CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Using this method on our data provides the desired outcome.

-- COMMAND ----------

SELECT foods_i_like(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC While the example provided here are simple string methods, these same basic principles can be used to add custom computations and logic for native execution in Spark SQL. 
-- MAGIC 
-- MAGIC Especially for enterprises that might be migrating users from systems with many defined procedures or custom-defined formulas, SQL UDFs can allow a handful of users to define the complex logic needed for common reporting and analytic queries.

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
