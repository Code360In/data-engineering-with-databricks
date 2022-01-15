-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md # Cleaning Data
-- MAGIC 
-- MAGIC Apply a number of common transformations to clean data with Spark SQL.
-- MAGIC 
-- MAGIC ##### Objectives
-- MAGIC - Summarize datasets and describe null behaviors
-- MAGIC - Retrieve and remove duplicates based on select columns
-- MAGIC - Validate datasets for expected counts, missing values, and duplicate records
-- MAGIC - Apply common transformations to clean and transform data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/setup-updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We'll work with new users records in `users_update` table for this lesson.

-- COMMAND ----------

SELECT * FROM users_update

-- COMMAND ----------

-- MAGIC %md ## Transformations 
-- MAGIC As we inspect and clean our data, we'll need to construct various column expressions and queries to express transformations to apply on our dataset.  
-- MAGIC - Column expressions are constructed from existing columns, operators, and built-in Spark SQL functions. They can be used in `SELECT` statements to express transformations that create new columns from datasets. 
-- MAGIC - Along with `SELECT`, many additional query commands can be used to express transformations in Spark SQL, including `WHERE`, `DISTINCT`, `ORDER BY`, `GROUP BY`, etc.

-- COMMAND ----------

-- MAGIC %md ## Inspect Data
-- MAGIC We'll inspect `users_update` for missing values and duplicate records. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Missing Values
-- MAGIC Count the number of missing values in each column of `users_update`. Note that nulls behave incorrectly in some math functions, including `count`.
-- MAGIC 
-- MAGIC #### Null Behavior with `count()`
-- MAGIC - `count(col)` skips `NULL` values when counting specific columns or expressions.
-- MAGIC - `count(*)` is a special case that counts the total number of rows without skipping `NULL` values
-- MAGIC 
-- MAGIC To count null values, use the `count_if` function or `WHERE` clause to provide a condition that filters for records where the value `IS NULL`.

-- COMMAND ----------

SELECT
  count_if(user_id IS NULL) missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) missing_timestamps, 
  count_if(email IS NULL) missing_emails
FROM users_update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Distinct Records
-- MAGIC - `DISTINCT *` returns rows with duplicates removed.
-- MAGIC - `DISTINCT col` returns unique values in column `col`
-- MAGIC 
-- MAGIC Note that the count for distinct users is greater than the count for distinct rows; rows containing `NULL` values were skipped from processing distinct rows. `count(*)` is the only case where `count()` includes records with `NULL` values.

-- COMMAND ----------

SELECT 
  count(user_id) total_ids, count(DISTINCT user_id) unique_ids,
  count(email) total_emails, count(DISTINCT email) unique_emails,
  count(*) total_rows, count(DISTINCT *) unique_non_null_rows
FROM users_update

-- COMMAND ----------

-- MAGIC %md ## Deduplicate Rows
-- MAGIC 
-- MAGIC Use `DISTINCT *` to remove true duplicate records (rows with same values for all columns).

-- COMMAND ----------

SELECT DISTINCT * FROM users_update

-- COMMAND ----------

-- MAGIC %md  #### Deduplicate Based on Specific Columns
-- MAGIC 
-- MAGIC Use `GROUP BY` to remove duplicate records based on select columns. The query below groups rows by `user_id` to deduplicate rows based on values from this column.
-- MAGIC 
-- MAGIC The `max()` aggregate function is used on the `email` column as a hack to capture non-null emails when multiple records are present.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) email, max(updated) updated
FROM users_update
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %md ## Validate Datasets
-- MAGIC Let's check our datasets for expected counts, missing values, and duplicate records.  
-- MAGIC Validation can be performed with simple filters and `WHERE` clauses (and `COUNT_IF` statements).

-- COMMAND ----------

-- MAGIC %md Validate that the `user_id` for each row is unique.

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %md Confirm that each email is associated with at most one `user_id`.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %md ## Date Format and Regex
-- MAGIC - Format datetimes as strings
-- MAGIC - Use `regexp_extract` to extract domains from the email column using regex

-- COMMAND ----------

SELECT *,
  date_format(first_touch, "MMM d, yyyy") first_touch_date,
  date_format(first_touch, "HH:mm:ss") first_touch_time,
  regexp_extract(email, "(?<=@)[^.]+(?=\.)", 0) email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
