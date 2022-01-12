-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Insert, Update, Delete

-- COMMAND ----------

-- MAGIC %run ../Includes/classic-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC We have a table, let's perform three operations on that table. These are fairly common operations on tables:
-- MAGIC - Inserting new records
-- MAGIC - Updating existing records
-- MAGIC - Deleting records

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), address VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's start with inserting items into our table. The cell above will create a simple table with two columns. Let's examine it.

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There should be no results, because our table has no values. Let's insert some values in!

-- COMMAND ----------

-- Insert into does an insert into our created table
INSERT INTO students VALUES
    ('Issac Newton', '123 Main Ave, San Jose', 3145);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now when we query that same table, we should see some records.

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- Let's add another 
INSERT INTO students VALUES
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Oh wait - we might see that our values were incorrect. In order to update a value, we can use a few different methods. The first is to `UPDATE`.

-- COMMAND ----------

INSERT OVERWRITE students VALUES
    ('Issac Newton', '123 Main Ave, Woolsthorpe', 3145),
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This makes sense if we're updating a very small table. However, we most likely want to take another approach to just update values:

-- COMMAND ----------

UPDATE students
  SET student_id = 3142
  WHERE student_id > 3000;

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC This gives us a simple way to update values. But what if the record is damaged and should be deleted? For this, we need a predicate. A predicate is a statement in SQL that can be evaluated and return back a true or false value. For example, if I say "is this person's name Ada Lovelace?" there are places where that condition is true, and places where that condition is false.
-- MAGIC 
-- MAGIC By including a predicate, we're allowing ourselves to not delete the entire table, and instead deletes just a record. Like so:

-- COMMAND ----------

DELETE FROM students WHERE name = 'Issac Newton';

-- COMMAND ----------

-- Now, running our select all statement shows that we've removed a record
SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A little clean up: run this next cell to drop our table.

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
