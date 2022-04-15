# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Just Enough Python for Databricks SQL
# MAGIC 
# MAGIC While Databricks SQL provides an ANSI-compliant flavor of SQL with many additional custom methods (including the entire Delta Lake SQL syntax), users migrating from some systems may run into missing features, especially around control flow and error handling.
# MAGIC 
# MAGIC Databricks notebooks allow users to write SQL and Python and execute logic cell-by-cell. PySpark has extensive support for executing SQL queries, and can easily exchange data with tables and temporary views.
# MAGIC 
# MAGIC Mastering just a handful of Python concepts will unlock powerful new design practices for engineers and analysts proficient in SQL. Rather than trying to teach the entire language, this lesson focuses on those features that can immediately be leveraged to write more extensible SQL programs on Databricks.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Print and manipulate multi-line Python strings
# MAGIC * Define variables and functions
# MAGIC * Use f-strings for variable substitution

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Strings
# MAGIC Characters enclosed in single (**`'`**) or double (**`"`**) quotes are considered strings.

# COMMAND ----------

"This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC To preview how a string will render, we can call **`print()`**.

# COMMAND ----------

print("This is a string")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC By wrapping a string in triple quotes (**`"""`**), it's possible to use multiple lines.

# COMMAND ----------

print("""
This 
is 
a 
multi-line 
string
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC This makes it easy to turn SQL queries into Python strings.

# COMMAND ----------

print("""
SELECT *
FROM test_table
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC When we execute SQL from a Python cell, we will pass a string as an argument to **`spark.sql()`**.

# COMMAND ----------

spark.sql("SELECT 1 AS test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC To render a query the way it would appear in a normal SQL notebook, we call **`display()`** on this function.

# COMMAND ----------

display(spark.sql("SELECT 1 AS test"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **NOTE**: Executing a cell with only a Python string in it will just print the string. Using **`print()`** with a string just renders it back to the notebook.
# MAGIC 
# MAGIC To execute a string that contains SQL using Python, it must be passed within a call to **`spark.sql()`**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Variables
# MAGIC Python variables are assigned using the **`=`**.
# MAGIC 
# MAGIC Python variable names need to start with a letter, and can only contain letters, numbers, and underscores. (Variable names starting with underscores are valid but typically reserved for special use cases.)
# MAGIC 
# MAGIC Many Python programmers favor snake casing, which uses only lowercase letters and underscores for all variables.
# MAGIC 
# MAGIC The cell below creates the variable **`my_string`**.

# COMMAND ----------

my_string = "This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Executing a cell with this variable will return its value.

# COMMAND ----------

my_string

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The output here is the same as if we typed **`"This is a string"`** into the cell and ran it.
# MAGIC 
# MAGIC Note that the quotation marks aren't part of the string, as shown when we print it.

# COMMAND ----------

print(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC This variable can be used the same way a string would be.
# MAGIC 
# MAGIC String concatenation (joining to strings together) can be performed with a **`+`**.

# COMMAND ----------

print("This is a new string and " + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC We can join string variables with other string variables.

# COMMAND ----------

new_string = "This is a new string and "
print(new_string + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Functions
# MAGIC Functions allow you to specify local variables as arguments and then apply custom logic. We define a function using the keyword **`def`** followed by the function name and, enclosed in parentheses, any variable arguments we wish to pass into the function. Finally, the function header has a **`:`** at the end.
# MAGIC 
# MAGIC Note: In Python, indentation matters. You can see in the cell below that the logic of the function is indented in from the left margin. Any code that is indented to this level is part of the function.
# MAGIC 
# MAGIC The function below takes one argument (**`arg`**) and then prints it.

# COMMAND ----------

def print_string(arg):
    print(arg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC When we pass a string as the argument, it will be printed.

# COMMAND ----------

print_string("foo")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC We can also pass a variable as an argument.

# COMMAND ----------

print_string(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Oftentimes we want to return the results of our function for use elsewhere. For this we use the **`return`** keyword.
# MAGIC 
# MAGIC The function below constructs a new string by concatenating our argument. Note that both functions and arguments can have arbitrary names, just like variables (and follow the same rules).

# COMMAND ----------

def return_new_string(string_arg):
    return "The string passed to this function was " + string_arg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Running this function returns the output.

# COMMAND ----------

return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Assigning it to a variable captures the output for reuse elsewhere.

# COMMAND ----------

function_output = return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC This variable doesn't contain our function, just the results of our function (a string).

# COMMAND ----------

function_output

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## F-strings
# MAGIC By adding the letter **`f`** before a Python string, you can inject variables or evaluated Python code by inserted them inside curly braces (**`{}`**).
# MAGIC 
# MAGIC Evaluate the cell below to see string variable substitution.

# COMMAND ----------

f"I can substitute {my_string} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The following cell inserts the string returned by a function.

# COMMAND ----------

f"I can substitute functions like {return_new_string('foobar')} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Combine this with triple quotes and you can format a paragraph or list, like below.

# COMMAND ----------

multi_line_string = f"""
I can have many lines of text with variable substitution:
  - A variable: {my_string}
  - A function output: {return_new_string('foobar')}
"""

print(multi_line_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Or you could format a SQL query.

# COMMAND ----------

table_name = "users"
filter_clause = "WHERE state = 'CA'"

query = f"""
SELECT *
FROM {table_name}
{filter_clause}
"""

print(query)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
