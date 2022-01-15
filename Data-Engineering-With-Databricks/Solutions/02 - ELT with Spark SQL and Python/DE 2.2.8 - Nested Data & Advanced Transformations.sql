-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md # Advanced Transformations
-- MAGIC 
-- MAGIC Manipulate nested data and work with advanced functions in SQL.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Flatten nested data
-- MAGIC - Apply advanced functions to transform data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-transactions

-- COMMAND ----------

-- MAGIC %md We will work with the `events_raw` table we loaded earlier from `event-kafka.json`.  

-- COMMAND ----------

SELECT * FROM events_raw

-- COMMAND ----------

-- MAGIC %md Earlier in the module, we merged a clean version of this new dataset, `events_update` into our original `events` dataset. The raw events data was cleaned by parsing and flattening the relevant JSON data in the `value` column, and then removing duplicate event records. We'll go through that process here.

-- COMMAND ----------

-- MAGIC %md ## Parse JSON
-- MAGIC  
-- MAGIC Parse JSON string with `from_json` and schema definition.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_raw_json AS 
SELECT from_json(cast(value as STRING), ("device STRING, ecommerce STRUCT< purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT< city: STRING, state: STRING>, items ARRAY< STRUCT< coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING")) json
FROM events_raw;

-- COMMAND ----------

-- MAGIC %md ## Flatten Nested Data
-- MAGIC Deduplicate events by `user_id` and `event_timestamp`.
-- MAGIC 
-- MAGIC Promote subfields with `json.*` to flatten data.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_events AS 
WITH deduped_events_raw AS (
  SELECT max(json) json FROM events_raw_json
  GROUP BY json.user_id, json.event_timestamp
)
SELECT json.* FROM deduped_events_raw

-- COMMAND ----------

-- MAGIC %md ## Higher Order Functions
-- MAGIC Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, records are frequently stored as array or map type objects. Higher-order functions allow you to transform data while preserving the original structure.
-- MAGIC 
-- MAGIC Higher order functions include:
-- MAGIC - `FILTER` filters an array using the given lambda function.
-- MAGIC - `EXIST` tests whether a statement is true for one or more elements in an array. 
-- MAGIC - `TRANSFORM` uses the given lambda function to transform all elements in an array.
-- MAGIC - `REDUCE` takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filter
-- MAGIC Remove items that are not king-sized from all records in our `items` column. We can use the `FILTER` function to create a new column that excludes that value from each array.
-- MAGIC 
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC 
-- MAGIC In the statement above:
-- MAGIC - **`FILTER`** : the name of the higher-order function <br>
-- MAGIC - **`items`** : the name of our input array <br>
-- MAGIC - **`i`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC - **`->`** :  Indicates the start of a function <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, `king_items`

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a `WHERE` clause to show only non-empty array values in the returned column. 
-- MAGIC 
-- MAGIC In this example, we accomplish that by using a subquery (a query within a query). They are useful for performing an operation in multiple steps. In this case, we're using it to create the named column that we will use with a `WHERE` clause. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform
-- MAGIC Built-in functions are designed to operate on a single, simple data type within a cell; they cannot process array values. `TRANSFORM` can be particularly useful when you want to apply an existing function to each element in an array. 
-- MAGIC 
-- MAGIC Compute the total revenue from king-sized items per order.
-- MAGIC 
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC 
-- MAGIC In the statement above, for each value in the input array, we extract the item's revenue value, multiply it by 100, and cast the result to integer. Note that we're using the same kind as references as in the previous command, but we name the iterator with a new variable, **`k`**.

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
