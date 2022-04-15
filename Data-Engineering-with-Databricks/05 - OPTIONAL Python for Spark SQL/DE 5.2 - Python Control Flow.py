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
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Leverage **`if`** / **`else`**
# MAGIC * Describe how errors impact notebook execution
# MAGIC * Write simple tests with **`assert`**
# MAGIC * Use **`try`** / **`except`** to handle errors

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## if/else
# MAGIC 
# MAGIC **`if`** / **`else`** clauses are common in many programming languages.
# MAGIC 
# MAGIC Note that SQL has the **`CASE WHEN ... ELSE`** construct, which is similar.
# MAGIC 
# MAGIC <strong>If you're seeking to evaluate conditions within your tables or queries, use **`CASE WHEN`**.</strong>
# MAGIC 
# MAGIC Python control flow should be reserved for evaluating conditions outside of your query.
# MAGIC 
# MAGIC More on this later. First, an example with **`"beans"`**.

# COMMAND ----------

food = "beans"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Working with **`if`** and **`else`** is all about evaluating whether or not certain conditions are true in your execution environment.
# MAGIC 
# MAGIC Note that in Python, we have the following comparison operators:
# MAGIC 
# MAGIC | Syntax | Operation |
# MAGIC | --- | --- |
# MAGIC | **`==`** | equals |
# MAGIC | **`>`** | greater than |
# MAGIC | **`<`** | less than |
# MAGIC | **`>=`** | greater than or equal |
# MAGIC | **`<=`** | less than or equal |
# MAGIC | **`!=`** | not equal |
# MAGIC 
# MAGIC If you read the sentence below out loud, you will be describing the control flow of your program.

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC As expected, because the variable **`food`** is the string literal **`"beans"`**, the **`if`** statement evaluated to **`True`** and the first print statement evaluated.
# MAGIC 
# MAGIC Let's assign a different value to the variable.

# COMMAND ----------

food = "beef"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Now the first condition will evaluate as **`False`**. 
# MAGIC 
# MAGIC What do you think will happen when you run the following cell?

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note that each time we assign a new value to a variable, this completely erases the old variable.

# COMMAND ----------

food = "potatoes"
print(food)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The Python keyword **`elif`** (short for **`else`** + **`if`**) allows us to evaluate multiple conditions.
# MAGIC 
# MAGIC Note that conditions are evaluated from top to bottom. Once a condition evaluates to true, no further conditions will be evaluated.
# MAGIC 
# MAGIC **`if`** / **`else`** control flow patterns:
# MAGIC 1. Must contain an **`if`** clause
# MAGIC 1. Can contain any number of **`elif`** clauses
# MAGIC 1. Can contain at most one **`else`** clause

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
elif food == "potatoes":
    print(f"My favorite vegetable is {food}")
elif food != "beef":
    print(f"Do you have any good recipes for {food}?")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC By encapsulating the above logic in a function, we can reuse this logic and formatting with arbitrary arguments rather than referencing globally-defined variables.

# COMMAND ----------

def foods_i_like(food):
    if food == "beans":
        print(f"I love {food}")
    elif food == "potatoes":
        print(f"My favorite vegetable is {food}")
    elif food != "beef":
        print(f"Do you have any good recipes for {food}?")
    else:
        print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Here, we pass the string **`"bread"`** to the function.

# COMMAND ----------

foods_i_like("bread")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC As we evaluate the function, we locally assign the string **`"bread"`** to the **`food`** variable, and the logic behaves as expected.
# MAGIC 
# MAGIC Note that we don't overwrite the value of the **`food`** variable as previously defined in the notebook.

# COMMAND ----------

food

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## try/except
# MAGIC 
# MAGIC While **`if`** / **`else`** clauses allow us to define conditional logic based on evaluating conditional statements, **`try`** / **`except`** focuses on providing robust error handling.
# MAGIC 
# MAGIC Let's begin by considering a simple function.

# COMMAND ----------

def three_times(number):
    return number * 3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Let's assume that the desired use of this function is to multiply an integer value by 3.
# MAGIC 
# MAGIC The below cell demonstrates this behavior.

# COMMAND ----------

three_times(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note what happens if a string is passed to the function.

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC In this case, we don't get an error, but we also do not get the desired outcome.
# MAGIC 
# MAGIC **`assert`** statements allow us to run simple tests of Python code. If an **`assert`** statement evaluates to true, nothing happens. 
# MAGIC 
# MAGIC If it evaluates to false, an error is raised.
# MAGIC 
# MAGIC Run the following cell to assert that the number **`2`** is an integer

# COMMAND ----------

assert type(2) == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Uncomment the following cell and then run it to assert that the string **`"2"`"** is an integer.
# MAGIC 
# MAGIC It should throw an **`AssertionError`**.

# COMMAND ----------

# assert type("2") == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC As expected, the string **`"2"`** is not an integer.
# MAGIC 
# MAGIC Python strings have a property to report whether or not they can be safely cast as numeric value as seen below.

# COMMAND ----------

assert "2".isnumeric()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC String numbers are common; you may see them as results from an API query, raw records in a JSON or CSV file, or returned by a SQL query.
# MAGIC 
# MAGIC **`int()`** and **`float()`** are two common methods for casting values to numeric types. 
# MAGIC 
# MAGIC An **`int`** will always be a whole number, while a **`float`** will always have a decimal.

# COMMAND ----------

int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC While Python will gladly cast a string containing numeric characters to a numeric type, it will not allow you to change other strings to numbers.
# MAGIC 
# MAGIC Uncomment the following cell and give it a try:

# COMMAND ----------

# int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note that errors will stop the execution of a notebook script; all cells after an error will be skipped when a notebook is scheduled as a production job.
# MAGIC 
# MAGIC If we enclose code that might throw an error in a **`try`** statement, we can define alternate logic when an error is encountered.
# MAGIC 
# MAGIC Below is a simple function that demonstrates this.

# COMMAND ----------

def try_int(num_string):
    try:
        int(num_string)
        result = f"{num_string} is a number."
    except:
        result = f"{num_string} is not a number!"
        
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC When a numeric string is passed, the function will return the result as an integer.

# COMMAND ----------

try_int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC When a non-numeric string is passed, an informative message is printed out.
# MAGIC 
# MAGIC **NOTE**: An error is **not** raised, even though an error occurred, and no value was returned. Implementing logic that suppresses errors can lead to logic silently failing.

# COMMAND ----------

try_int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Below, our earlier function is updated to include logic for handling errors to return an informative message.

# COMMAND ----------

def three_times(number):
    try:
        return int(number) * 3
    except ValueError as e:
        print(f"You passed the string variable '{number}'.\n")
        print(f"Try passing an integer instead.")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Now our function can process numbers passed as strings.

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC And prints an informative message when a string is passed.

# COMMAND ----------

three_times("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note that as implemented, this logic would only be useful for interactive execution of this logic (the message isn't currently being logged anywhere, and the code will not return the data in the desired format; human intervention would be required to act upon the printed message).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Applying Python Control Flow for SQL Queries
# MAGIC 
# MAGIC While the above examples demonstrate the basic principles of using these designs in Python, the goal of this lesson is to learn how to apply these concepts to executing SQL logic on Databricks.
# MAGIC 
# MAGIC Let's revisit converting a SQL cell to execute in Python.
# MAGIC 
# MAGIC **NOTE**: The following setup script ensures an isolated execution environment.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_tmp_vw(id, name, value) AS VALUES
# MAGIC   (1, "Yve", 1.0),
# MAGIC   (2, "Omar", 2.5),
# MAGIC   (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Run the SQL cell below to preview the contents of this temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Running SQL in a Python cell simply requires passing the string query to **`spark.sql()`**.

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC But recall that executing a query with **`spark.sql()`** returns the results as a DataFrame rather than displaying them; below, the code is augmented to capture the result and display it.

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Using a simple **`if`** clause with a function allows us to execute arbitrary SQL queries, optionally displaying the results, and always returning the resultant DataFrame.

# COMMAND ----------

def simple_query_function(query, preview=True):
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

result = simple_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Below, we execute a different query and set preview to **`False`**, as the purpose of the query is to create a temp view rather than return a preview of data.

# COMMAND ----------

new_query = "CREATE OR REPLACE TEMP VIEW id_name_tmp_vw AS SELECT id, name FROM demo_tmp_vw"

simple_query_function(new_query, preview=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC We now have a simple extensible function that could be further parameterized depending on the needs of our organization.
# MAGIC 
# MAGIC For example, suppose we want to protect our company from malicious SQL, like the query below.

# COMMAND ----------

injection_query = "SELECT * FROM demo_tmp_vw; DROP DATABASE prod_db CASCADE; SELECT * FROM demo_tmp_vw"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC We can use the **`find()`** method to test for multiple SQL statements by looking for a semicolon.

# COMMAND ----------

injection_query.find(";")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC If it's not found it will return **`-1`**

# COMMAND ----------

injection_query.find("x")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC With that knowledge, we can define a simple search for a semicolon in the query string and raise a custom error message if it was found (not **`-1`**)

# COMMAND ----------

def injection_check(query):
    semicolon_index = query.find(";")
    if semicolon_index >= 0:
        raise ValueError(f"Query contains semi-colon at index {semicolon_index}\nBlocking execution to avoid SQL injection attack")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **NOTE**: The example shown here is not sophisticated, but seeks to demonstrate a general principle. 
# MAGIC 
# MAGIC Always be wary of allowing untrusted users to pass text that will be passed to SQL queries. 
# MAGIC 
# MAGIC Also note that only one query can be executed using **`spark.sql()`**, so text with a semi-colon will always throw an error.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Uncomment the following cell and give it a try:

# COMMAND ----------

# injection_check(injection_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC If we add this method to our earlier query function, we now have a more robust function that will assess each query for potential threats before execution.

# COMMAND ----------

def secure_query_function(query, preview=True):
    injection_check(query)
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC As expected, we see normal performance with a safe query.

# COMMAND ----------

secure_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC But prevent execution when when bad logic is run.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
