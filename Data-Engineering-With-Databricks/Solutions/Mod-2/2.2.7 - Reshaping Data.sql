-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Reshaping Data
-- MAGIC 
-- MAGIC Combine datasets and lookup tables with various join types and strategies.
-- MAGIC Reshape with pivot tables.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Combine datasets using different types of joins
-- MAGIC - Join records to a pre-existing lookup table
-- MAGIC - Examine and provide hints for join strategies
-- MAGIC - Reshape data using pivot tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-complex

-- COMMAND ----------

-- MAGIC %md ## Explode Arrays
-- MAGIC Use the `explode` array function to explode arrays in the item field of events.

-- COMMAND ----------

SELECT *, explode(items) item FROM events_clean

-- COMMAND ----------

-- MAGIC %md ## Collect Arrays
-- MAGIC Explode and flatten complex data using various array functions

-- COMMAND ----------

SELECT user_id, 
  flatten(collect_set(items.item_id)) cart_history
  collect_set(event_name) event_history
FROM (SELECT *,  explode(items) FROM events_clean)

-- COMMAND ----------

-- MAGIC %md ## Join a Lookup Table
-- MAGIC 
-- MAGIC Lookup tables are normally small, historical tables used to enrich new data passing through an ETL pipeline.
-- MAGIC 
-- MAGIC In this example, we will use a small lookup table to get details for each item sold by this retailer.
-- MAGIC 
-- MAGIC **Inner Join:** Selects rows that have matching values in both relations. This is the default join in Spark SQL.

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_enriched AS
SELECT a.user_id, b.item
FROM (SELECT *, explode(items) items_exploded FROM sales) a
INNER JOIN item_lookup b
ON a.item_id = b.item_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Examine Join Strategy
-- MAGIC Use the `EXPLAIN` command to view the physical plan used to execute the query.   
-- MAGIC Look for BroadcastHashJoin or BroadcastExchange.

-- COMMAND ----------

EXPLAIN FORMATTED 
SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC By default, Spark performed a broadcast join rather than a shuffle join. That is, it broadcasted `item_lookup` to the larger `carts_exploded`, replicating the smaller dataset on each node of our cluster. This avoided having to move the larger dataset across the cluster.

-- COMMAND ----------

-- MAGIC %md `autoBroadcastJoinThreshold`
-- MAGIC 
-- MAGIC We can access configuration settings to take a look at the broadcast join threshold. This specifies the maximum size in bytes for a table that broadcasts to worker nodes.

-- COMMAND ----------

SET spark.sql.autoBroadcastJoinThreshold

-- COMMAND ----------

-- MAGIC %md Re-examine physical plan when join is executed while broadcasting is disabled
-- MAGIC 1. Drop threshold to `-1` to disable broadcasting
-- MAGIC 1. Explain join
-- MAGIC 
-- MAGIC Now notice the lack of broadcast in the query physical plan.

-- COMMAND ----------

SET spark.sql.autoBroadcastJoinThreshold=-1

-- COMMAND ----------

-- MAGIC %md Notice a sort merge join is performed, rather than a broadcast join.

-- COMMAND ----------

EXPLAIN FORMATTED 
SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Broadcast Join Hint
-- MAGIC Use a join hint to suggest broadcasting the lookup table for the join.
-- MAGIC 
-- MAGIC The join side with this hint will be broadcast regardless of `autoBroadcastJoinThreshold`.

-- COMMAND ----------

EXPLAIN FORMATTED
SELECT /*+ BROADCAST(b) */ a.user_id, b.item
FROM (SELECT *, explode(items) items_exploded FROM sales) a
INNER JOIN item_lookup b
ON a.item_id = b.item_id

-- COMMAND ----------

-- MAGIC %md Reset the original threshold.

-- COMMAND ----------

SET spark.sql.autoBroadcastJoinThreshold=10485760b

-- COMMAND ----------

-- MAGIC %md ## Pivot Tables
-- MAGIC The `PIVOT` clause is used for data perspective. We can get the aggregated values based on specific column values, which will be turned to multiple columns used in `SELECT` clause. The `PIVOT` clause can be specified after the table name or subquery.
-- MAGIC 
-- MAGIC **`SELECT * FROM ()`**: The `SELECT` statement inside the parentheses is the input for this table.
-- MAGIC 
-- MAGIC **`PIVOT`**: The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the pivot column in the `FOR` subclause. The `IN` operator contains the pivot column values. <br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use `PIVOT` to create a new `transactions` table that flattens out the information contained in the `sales` table and joins this with `users` table. 
-- MAGIC 
-- MAGIC Join these tables on email address, but without propagating this email address forward (to avoid potential PII exposure in downstream tables).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW transactions AS

SELECT * FROM (
  SELECT
    user_id,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    a.items_exploded.item_id item_id,
    a.items_exploded.quantity quantity
  FROM sales_enriched a
  INNER JOIN users b 
  ON a.email = b.email
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
