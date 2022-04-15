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
-- MAGIC # Reshaping Data Lab
-- MAGIC 
-- MAGIC In this lab, you will create a **`clickpaths`** table that aggregates the number of times each user took a particular action in **`events`** and then join this information with the flattened view of **`transactions`** created in the previous notebook.
-- MAGIC 
-- MAGIC You'll also explore a new higher order function to flag items recorded in **`sales`** based on information extracted from item names.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Pivot and join tables to create clickpaths for each user
-- MAGIC - Apply higher order functions to flag types of products purchased

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.9L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Reshape Datasets to Create Click Paths
-- MAGIC This operation will join data from your **`events`** and **`transactions`** tables in order to create a record of all actions a user took on the site and what their final order looked like.
-- MAGIC 
-- MAGIC The **`clickpaths`** table should contain all the fields from your **`transactions`** table, as well as a count of every **`event_name`** in its own column. Each user that completed a purchase should have a single row in the final table. Let's start by pivoting the **`events`** table to get counts for each **`event_name`**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### 1. Pivot **`events`** to count actions for each user
-- MAGIC We want to aggregate the number of times each user performed a specific event, specified in the **`event_name`** column. To do this, group by **`user`** and pivot on **`event_name`** to provide a count of every event type in its own column, resulting in the schema below.
-- MAGIC 
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC 
-- MAGIC A list of the event names are provided below.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE VIEW events_pivot
<FILL_IN>
("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The helper functions below will return an error with a message on what needs to change if you have not followed instructions. No output means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, column_names, num_rows):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert spark.table(table_name).columns == column_names, "Please name the columns in the order provided above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_columns = ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium']
-- MAGIC check_table_results("events_pivot", event_columns, 204586)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### 2. Join event counts and transactions for all users
-- MAGIC 
-- MAGIC Next, join **`events_pivot`** with **`transactions`** to create the table **`clickpaths`**. This table should have the same event name columns from the **`events_pivot`** table created above, followed by columns from the **`transactions`** table, as shown below.
-- MAGIC 
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- TODO
CREATE OR REPLACE VIEW clickpaths AS
<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clickpath_columns = event_columns + ['user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K']
-- MAGIC check_table_results("clickpaths", clickpath_columns, 9085)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Flag Types of Products Purchased
-- MAGIC Here, you'll use the higher order function **`EXISTS`** to create boolean columns **`mattress`** and **`pillow`** that indicate whether the item purchased was a mattress or pillow product.
-- MAGIC 
-- MAGIC For example, if **`item_name`** from the **`items`** column ends with the string **`"Mattress"`**, the column value for **`mattress`** should be **`true`** and the value for **`pillow`** should be **`false`**. Here are a few examples of items and the resulting values.
-- MAGIC 
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC 
-- MAGIC See documentation for the <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> function.  
-- MAGIC You can use the condition expression **`item_name LIKE "%Mattress"`** to check whether the string **`item_name`** ends with the word "Mattress".

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE sales_product_flags AS
<FILL_IN>
EXISTS <FILL_IN>.item_name LIKE "%Mattress"
EXISTS <FILL_IN>.item_name LIKE "%Pillow"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", ['items', 'mattress', 'pillow'], 10539)
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 10015, 'num_pillow': 1386}, "There should be 10015 rows where mattress is true, and 1386 where pillow is true"

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
