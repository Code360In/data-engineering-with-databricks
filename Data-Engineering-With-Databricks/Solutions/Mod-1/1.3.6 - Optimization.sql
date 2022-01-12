-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimizing Delta

-- COMMAND ----------

-- MAGIC %run ../Includes/classic-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's set up our table.

-- COMMAND ----------

DROP TABLE IF EXISTS students;
CREATE TABLE IF NOT EXISTS students (name VARCHAR(64), address VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

INSERT INTO students VALUES
    ('Issac Newton', '123 Main Ave, Woolsthorpe', 3145),
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Optimization with Delta is achieved by running the `OPTIMIZE` command. There's not much to it!

-- COMMAND ----------

OPTIMIZE students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC A Bloom filter index is a space-efficient data structure that enables data skipping on chosen columns, particularly for fields containing arbitrary text. The Bloom filter operates by either stating that data is definitively not in the file, or that it is probably in the file, with a defined false positive probability (FPP).
-- MAGIC 
-- MAGIC Creating a Bloom filter index is achieved by running:

-- COMMAND ----------

CREATE BLOOMFILTER INDEX
ON TABLE students
FOR COLUMNS(name OPTIONS (fpp=0.1, numItems=2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `for columns` on line 3 lets Delta know which column should be used for creating the Bloom filter. 
-- MAGIC 
-- MAGIC 
-- MAGIC Additionally, we set an FPP. The size of a Bloom filter depends on the number elements in the set for which the Bloom filter has been created and the required FPP. The lower the FPP, the higher the number of used bits per element and the more accurate it will be, at the cost of more disk space and slower downloads. For example, an FPP of 10% requires 5 bits per element. 

-- COMMAND ----------

-- Cleanup
DROP TABLE students

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
