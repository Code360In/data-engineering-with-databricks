-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Merge Into

-- COMMAND ----------

-- MAGIC %run ../Includes/classic-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `MERGE INTO` will merge a set of updates, insertions, and deletions based on a source table into a target Delta table.
-- MAGIC 
-- MAGIC Let's start by creating a table

-- COMMAND ----------

-- Create our table
CREATE TABLE students (name VARCHAR(64), address VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

-- Insert some values 
INSERT INTO students VALUES
    ('Issac Newton', '123 Main Ave, San Jose', 3141),
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we set up our target table

-- COMMAND ----------

-- Create our second table
CREATE TABLE study (field VARCHAR(64), description VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

-- Insert some values into second table
INSERT INTO study VALUES
    ('Calculus', 'Mathematics', 3141),
    ('Computer Programming', 'Calculations on big data', 2718);

-- COMMAND ----------

MERGE INTO students 
USING study
ON students.student_id = study.student_id
WHEN MATCHED
  THEN DELETE *;

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

DROP TABLE study

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
