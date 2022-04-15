-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Views and CTEs on Databricks, Cont'
-- MAGIC 
-- MAGIC We are picking up from notebook [DE 3.2A - Views and CTEs on Databricks]($./DE 3.2A - Views and CTEs on Databricks) where we just reviewed the following two lists of tables<br/>
-- MAGIC and views with the special note that our global temp view **`global_temp_view_dist_gt_1000`** was not included in the first list.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="data:image/png;base64,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" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC With the Notebook's state reset, we need to re-initialize some of our lesson-specific configuration.
-- MAGIC 
-- MAGIC Note: We will **NOT** be recreating the database for the second 1/2 of this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.2B

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC But, we do need to configure this session to use our database by default.

-- COMMAND ----------

USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Finally, run the following two cells to confirm that:
-- MAGIC 1. The table **`external_table`** still exists.
-- MAGIC 2. The view **`view_delays_abq_lax`** still exists.
-- MAGIC 3. The temp view **`temp_view_delays_gt_120`** does **NOT** exist.
-- MAGIC 3. The global temp view **`global_temp_view_dist_gt_1000`** does exist in the special **`global_temp`** database.
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> Hint: If you were to go back to the previous notebook and run **`SHOW TABLES`**<br/>
-- MAGIC again, all three tables and views from the current database will still be shown.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC As mentioned previously, temp views are tied to a Spark session and as such are not accessible...
-- MAGIC * After restarting a cluster
-- MAGIC * After detaching and reataching to a cluster
-- MAGIC * After installing a python package which in turn restarts the Python interpreter
-- MAGIC * Or from another notebook
-- MAGIC 
-- MAGIC ...with the special exception of global temporary views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Global temp views behave much like other temporary views but differ in one important way. 
-- MAGIC 
-- MAGIC They are added to the **`global_temp`** database that exists on the **`cluster`**.
-- MAGIC 
-- MAGIC As long as the cluster is running, this database persists and any notebooks attached to the cluster can access its global temporary views.  
-- MAGIC   
-- MAGIC We can see this in action by running the follow cells:

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Global temp views are "lost" when the cluster is restarted.
-- MAGIC 
-- MAGIC Take our word for it, don't do it now, but if you were to restart the cluster, the above select statement would fail because the table would no longer exist.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Common Table Expressions (CTEs)
-- MAGIC CTEs can be used in a variety of contexts. Below, are a few examples of the different ways a CTE can be used in a query. First, an example of making multiple column aliases using a CTE.

-- COMMAND ----------

WITH flight_delays(
  total_delay_time,
  origin_airport,
  destination_airport
) AS (
  SELECT
    delay,
    origin,
    destination
  FROM
    external_table
)
SELECT
  *
FROM
  flight_delays
WHERE
  total_delay_time > 120
  AND origin_airport = "ATL"
  AND destination_airport = "DEN";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Next, is an example of a CTE in a CTE definition.

-- COMMAND ----------

WITH lax_bos AS (
  WITH origin_destination (origin_airport, destination_airport) AS (
    SELECT
      origin,
      destination
    FROM
      external_table
  )
  SELECT
    *
  FROM
    origin_destination
  WHERE
    origin_airport = 'LAX'
    AND destination_airport = 'BOS'
)
SELECT
  count(origin_airport) AS `Total Flights from LAX to BOS`
FROM
  lax_bos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Now, here is an example of a CTE in a subquery.

-- COMMAND ----------

SELECT
  max(total_delay) AS `Longest Delay (in minutes)`
FROM
  (
    WITH delayed_flights(total_delay) AS (
      SELECT
        delay
      FROM
        external_table
    )
    SELECT
      *
    FROM
      delayed_flights
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We can also use a CTE in a subquery expression.

-- COMMAND ----------

SELECT
  (
    WITH distinct_origins AS (
      SELECT DISTINCT origin FROM external_table
    )
    SELECT
      count(origin) AS `Number of Distinct Origins`
    FROM
      distinct_origins
  ) AS `Number of Different Origin Airports`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Finally, here is a CTE in a **`CREATE VIEW`** statement.

-- COMMAND ----------

CREATE OR REPLACE VIEW BOS_LAX 
AS WITH origin_destination(origin_airport, destination_airport) 
AS (SELECT origin, destination FROM external_table)
SELECT * FROM origin_destination
WHERE origin_airport = 'BOS' AND destination_airport = 'LAX';

SELECT count(origin_airport) AS `Number of Delayed Flights from BOS to LAX` FROM BOS_LAX;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Clean up 
-- MAGIC We first drop the training database.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE;

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
