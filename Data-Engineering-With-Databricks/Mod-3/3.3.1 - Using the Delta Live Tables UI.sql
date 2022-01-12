-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Using the Delta Live Tables UI
-- MAGIC 
-- MAGIC A notebook that provides an example Delta Live Tables pipeline to:
-- MAGIC 
-- MAGIC - Read raw JSON clickstream data into a table.
-- MAGIC - Read records from the raw data table and use a Delta Live Tables query and expectations to create a new table with cleaned and prepared data.
-- MAGIC - Perform an analysis on the prepared data with a Delta Live Tables query.

-- COMMAND ----------

CREATE LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
AS
SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/`

-- COMMAND ----------

CREATE LIVE TABLE clickstream_prepared(
  CONSTRAINT valid_current_page_title EXPECT (current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
AS
SELECT curr_title AS current_page_title, prev_title AS previous_page_title, 
  CAST(n AS INTEGER) click_count 
  FROM LIVE.clickstream_raw

-- COMMAND ----------

CREATE LIVE TABLE top_spark_referrers
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS
SELECT previous_page_title AS referrer, click_Count
  FROM LIVE.clickstream_prepared
  WHERE current_page_title = "Apache_Spark"
  ORDER BY click_count DESC
  LIMIT 10

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
