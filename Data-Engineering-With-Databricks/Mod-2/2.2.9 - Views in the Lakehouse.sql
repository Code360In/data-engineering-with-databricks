-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md # Views in the Lakehouse
-- MAGIC 
-- MAGIC Register views to create stable, secure queries in the Lakehouse.
-- MAGIC 
-- MAGIC In this lesson, we'll give a quick overview of how stored views are created and managed.
-- MAGIC 
-- MAGIC 
-- MAGIC Rather than trying to capture every possible metric in our view, we'll create a summary of values that might be of interest to our analysts.
-- MAGIC 
-- MAGIC 0. Views as saved queries
-- MAGIC 0. Aliasing tables to views
-- MAGIC 0. Dynamic views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-transform

-- COMMAND ----------

-- MAGIC %md ## Views as saved queries
-- MAGIC 
-- MAGIC A Spark DataFrame and a view are nearly identical constructs. By calling `EXPLAIN` on our DataFrame, we can see that our source table is a set of instructions to deserialize the files containing our data.

-- COMMAND ----------

EXPLAIN FORMATTED SELECT * FROM transactions

-- COMMAND ----------

-- MAGIC %md ## Aliasing Tables to Views
-- MAGIC 
-- MAGIC #### Examine Clickpath Data
-- MAGIC Define logic that creates or updates a table that aggregates the number of times each user took a particular action and then join this information with the flattened view of transactions created earlier. This will combine data from the `events_clean` and `transactions` tables in order to create a record of all actions a user took on the site and what their final order looked like.
-- MAGIC 
-- MAGIC This `clickpaths` table should contain all the fields from your `transactions` table, as well as a count of every `event_name` in its own column. Each user that completed a purchase should have a single row in the final table.

-- COMMAND ----------

CREATE OR REPLACE VIEW clickpaths AS
  WITH user_event_counts AS (
  SELECT * FROM (
    SELECT user_id user, event_name 
    FROM events_clean
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
-- MAGIC We can see that our view is simply storing the Spark plan for our query.

-- COMMAND ----------

EXPLAIN FORMATTED SELECT * FROM clickpaths

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we execute a query against this view, we will process the plan to generate the logically correct result.
-- MAGIC 
-- MAGIC Note that while the data may end up in the Delta Cache, this result is not guaranteed to be persisted, and is only cached for the currently active cluster.

-- COMMAND ----------

SELECT * FROM clickpaths WHERE login = True

-- COMMAND ----------

SELECT *
FROM clickpaths
WHERE user_id = "UA000000102360871"

-- COMMAND ----------

-- MAGIC %md ## Dynamic Views
-- MAGIC Databricks <a href="https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions" target="_blank">dynamic views</a> allow user or group identity ACLs to be applied to data at the column (or row) level.
-- MAGIC 
-- MAGIC Database administrators can configure data access privileges to disallow access to a source table and only allow users to query a redacted view. Users with sufficient privileges will be able to see all fields, while restricted users will be shown arbitrary results, as defined at view creation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Consider our `sales` table with the following columns.

-- COMMAND ----------

DESCRIBE sales

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_view AS
SELECT
  order_id,
  transaction_timestamp,
  CASE 
    WHEN is_member('ade_demo') THEN email
    ELSE 'REDACTED'
  END AS email,
  CASE 
    WHEN is_member('ade_demo') THEN purchase_revenue_in_usd
    ELSE 'REDACTED'
  END AS purchase_revenue_in_usd
FROM sales

-- COMMAND ----------

SELECT * FROM sales_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now when we query from `sales_view`, only members of the group `ade_demo` will be able to see results in plain text.
-- MAGIC 
-- MAGIC **NOTE:** You may not have privileges to create groups or assign membership. Your instructor should be able to demonstrate how group membership will change query results.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
