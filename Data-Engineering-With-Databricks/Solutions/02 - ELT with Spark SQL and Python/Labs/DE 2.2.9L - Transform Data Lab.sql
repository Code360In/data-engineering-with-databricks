-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Transform Data Lab
-- MAGIC 
-- MAGIC Reshape events and transactions to create clickpaths. 
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Validate deduplication of events
-- MAGIC - Reshape events and transactions to create clickpaths
-- MAGIC - Apply higher order functions to flag types of products purchased

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../../Includes/setup-transactions

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Validate Deduplication of Events

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_events FROM (
  SELECT user_id, event_timestamp, count(*) row_count
  FROM deduped_events
  GROUP BY user_id, event_timestamp)

-- COMMAND ----------

-- MAGIC %md ## Reshape events and transactions to create clickpaths
-- MAGIC - Pivot events for user events
-- MAGIC - Join transactions and user events

-- COMMAND ----------

CREATE OR REPLACE VIEW clickpaths AS
  WITH user_event_counts AS (
  SELECT * FROM (
    SELECT user_id user, event_name 
    FROM events
  ) PIVOT ( count(*) FOR event_name IN (
      "cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
      "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
      "cc_info", "foam", "reviews", "original", "delivery", "premium" )))
SELECT * 
FROM user_event_counts a
JOIN transactions b 
  ON a.user = b.user_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flag Mattress and Pillow Sales
-- MAGIC Flag all mattress and pillow sales. In the following query, each value is checked to see if the item_name ends with "Mattress." If so, it gets flagged into the new column, `mattress`. The same is done for "Pillow" as well.

-- COMMAND ----------

SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
