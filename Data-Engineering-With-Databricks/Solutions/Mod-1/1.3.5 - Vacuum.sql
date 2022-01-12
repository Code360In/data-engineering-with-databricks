-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Vacuum

-- COMMAND ----------

-- MAGIC %run ../Includes/classic-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `VACUUM` cleans up files associated with a table. There are different versions of this command for Delta and Apache Spark tables.

-- COMMAND ----------

DROP TABLE IF EXISTS students;
CREATE TABLE students (name VARCHAR(64), address VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

INSERT OVERWRITE students VALUES
    ('Issac Newton', '123 Main Ave, Woolsthorpe', 3145),
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC VACUUM will recursively vacuum directories associated with the Delta table and remove data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. Files are deleted according to the time they have been logically removed from Delta’s transaction log + retention hours, not their modification timestamps on the storage system. The default threshold is 7 days. 

-- COMMAND ----------

VACUUM students RETAIN 168 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC The `RETAIN` argument  will set a threshold for how long files not required anymore should last. 
-- MAGIC 
-- MAGIC If you select a value less than 168 by default, Delta will warn you.

-- COMMAND ----------

VACUUM students RETAIN 167 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC VACUUM takes an optional argument of DRY RUN. This will return a list of files to be deleted. This is a great idea to run!

-- COMMAND ----------

--- Clean up 
DROP TABLE students

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
