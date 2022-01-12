# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Just Enough Python for Databricks SQL Lab
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students should be able to:
# MAGIC * Review basic Python code and describe expected outcomes of code execution
# MAGIC * Reason through control flow statements in Python functions
# MAGIC * Add parameters to a SQL query by wrapping it in a Python function

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviewing Python Basics
# MAGIC 
# MAGIC In the previous notebook, we briefly explored using `spark.sql()` to execute arbitrary SQL commands from Python.
# MAGIC 
# MAGIC Look at the following 3 cells. Before executing each cell, identify:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment
# MAGIC 
# MAGIC Then execute the cells, compare the results to your expectations, and see the explanations below.

# COMMAND ----------

course = "python_for_sql"

# COMMAND ----------

spark.sql(f"SELECT '{course}'")

# COMMAND ----------

display(spark.sql(f"SELECT '{course}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. `Cmd 3` assigns a string to a variable. When a variable assignment is successful, no output is displayed to the notebook. A new variable is added to the current execution environment.
# MAGIC 1. `Cmd 4` executes a SQL query and returns the results as a DataFrame. In this case, the SQL query is just to select a string, so no changes to our environment occur. When a returned DataFrame is not captured, the schema for the DataFrame is displayed alongside the word `DataFrame`.
# MAGIC 1. `Cmd 5` executes the same SQL query and displays the returned DataFrame. This combination of `display()` and `spark.sql()` most closely mirrors executing logic in a `%sql` cell; the results will always be printed in a formatted table, assuming results are returned by the query; some queries will instead manipulate tables or databases, in which case the work `OK` will print to show successful execution. In this case, no changes to our environment occur from running this code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Up a Development Environment
# MAGIC 
# MAGIC Throughout this course, we use logic similar to the follow cell to capture information about the user currently executing the notebook and create an isolated development database.
# MAGIC 
# MAGIC The `re` library is the [standard Python library for regex](https://docs.python.org/3/library/re.html).
# MAGIC 
# MAGIC Databricks SQL has a special method to capture the username of the `current_user()`; and the `.first()[0]` code is a quick hack to capture the first row of the first column of a query executed with `spark.sql()` (in this case, we do this safely knowing that there will only be 1 row and 1 column).
# MAGIC 
# MAGIC All other logic below is just string formatting.

# COMMAND ----------

import re

username = spark.sql("SELECT current_user()").first()[0]
userhome = f"dbfs:/user/{username}/{course}"
database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we add a simple control flow statement to this logic to create and use this user-specific database. Optionally, we will reset this database and drop all of the contents on repeat execution. (Note the the default mode is `"reset"`).

# COMMAND ----------

def create_database(course, mode="reset"):
    import re

    username = spark.sql("SELECT current_user()").first()[0]
    userhome = f"dbfs:/user/{username}/{course}"
    database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

    print(f"""
        username: {username}
        userhome: {userhome}
        database: {database}
    """)
    
    if mode == "reset":
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
        dbutils.fs.rm(userhome, True)
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {database}
        LOCATION '{userhome}'
    """)
    spark.sql(f"USE {database}")
    
create_database(course)

# COMMAND ----------

# MAGIC %md
# MAGIC While this logic as defined is geared toward isolating students in shared workspaces for instructional purposes, the same basic design could be leveraged for testing new logic in an isolated environment before pushing to production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Errors Gracefully
# MAGIC 
# MAGIC Review the logic in the function below.
# MAGIC 
# MAGIC Note that we've just declared a new database that currently contains no tables.

# COMMAND ----------

def query_or_make_demo_table(table):
    try:
        display(spark.sql(f"SELECT * FROM {table}"))
    except:
        spark.sql(f"""
            CREATE TABLE {table}
            (id INT, name STRING, value DOUBLE, state STRING)
        """)
        spark.sql(f"""
            INSERT INTO {table}
            VALUES (1, "Yve", 1.0, "CA"),
              (2, "Omar", 2.5, "NY"),
              (3, "Elia", 3.3, "OH"),
              (4, "Rebecca", 4.7, "TX"),
              (5, "Ameena", 5.3, "CA"),
              (6, "Ling", 6.6, "NY"),
              (7, "Pedro", 7.1, "KY")
        """)
        display(spark.sql(f"SELECT * FROM {table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Try to identify the following before executing the next cell:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC Now answer the same three questions before running the same query below.

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC - On the first execution, the table `demo_table` did not yet exist. As such, the attempt to return the contents of the table created an error, which resulted in our `except` block of logic executing. This block:
# MAGIC   1. Created the table
# MAGIC   1. Inserted values
# MAGIC   1. Returned the contents of the table
# MAGIC - On the second execution, the table `demo_table` already exists, and so the first query in the `try` block executes without error. As a result, we just display the results of the query without modifying anything in our environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adapting SQL to Python
# MAGIC Let's consider the following SQL query against our demo table created above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, value 
# MAGIC FROM demo_table
# MAGIC WHERE state = "CA"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use this simple example to practice creating a Python function that adds optional functionality.
# MAGIC 
# MAGIC Our target function will:
# MAGIC * Always return only the `id` and `value` column from the a table named `demo_table`
# MAGIC * Allow filtering results by state, but default to all states
# MAGIC * Optionally return the query result object (a PySpark DataFrame)
# MAGIC 
# MAGIC Stretch Goal:
# MAGIC * Add logic to check that if the value passed for the `state` filter contains two uppercase letters
# MAGIC 
# MAGIC Some starter logic has been provided below.

# COMMAND ----------

# TODO
def preview_values(state=None, return_results=False):
    query = <FILL-IN>
    if state is not None:
        <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC The assert statements below can be used to check whether or not your function works as intended.

# COMMAND ----------

import pyspark.sql.dataframe

assert preview_values(return_results=True).columns == ["id", "value"], "Query should only return `id` and `value` columns"
assert preview_values() == None, "Function should not return anything by default"
assert type(preview_values(return_results=True)) == pyspark.sql.dataframe.DataFrame, "Function should optionally return the DataFrame results"
assert preview_values(state="OH", return_results=True).first()[0] == 3, "Function should allow filtering by state"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
