# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuring Privileges for Production Data and Derived Tables
# MAGIC 
# MAGIC The instructions as detailed below are provided for pairs of users to explore how Table ACLs on Databricks work. It leverages Databricks SQL and the Data Explorer to accomplish these tasks, and assumes that neither user has admin privileges for the workspace. An admin will need to have previously granted `CREATE` and `USAGE` privileges on a catalog for users to be able to create databases in Databricksd SQL
# MAGIC 
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use Data Explorer to navigate relational entities
# MAGIC * Configure permissions for tables and views with Data Explorer
# MAGIC * Configure minimal permissions to allow for table discovery and querying
# MAGIC * Change ownership for databases, tables, and views created in DBSQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exchange User Names with your Partner
# MAGIC If you are not in a workspace where you usernames correspond with your email address, make sure your partner has your username. They will need this when assigning privileges and searching for your database at later steps.
# MAGIC 
# MAGIC The following query will print your username.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Setup Statements
# MAGIC 
# MAGIC The following cell uses Python to extract username of the present user and format this into several statements used to create databases, tables, and views.
# MAGIC 
# MAGIC Both students should execute the following cell. Successful execution will print out a series of formatted SQL queries, which can be copied into the DBSQL query editor and executed.

# COMMAND ----------

def generate_query(course, mode="reset"):
    import re
    import random

    username = spark.sql("SELECT current_user()").first()[0]
    userhome = f"dbfs:/user/{username}/{course}"
    database = f"""dbacademy_{re.sub("[^a-zA-Z0-9]", "_", username)}_{course}"""
    
    if mode == "reset":
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
        dbutils.fs.rm(userhome, True)

    print(f"""
CREATE DATABASE IF NOT EXISTS {database}
LOCATION '{userhome}';

USE {database};
    
CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN); 

INSERT INTO beans
VALUES ('black', 'black', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('lentils', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('jelly', 'rainbow', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('pinto', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('green', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('beanbag chair', 'white', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('lentils', 'green', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('kidney', 'red', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])}),
('castor', 'brown', {random.uniform(0, 5000):.2f}, {random.choice(["true", "false"])});

CREATE VIEW tasty_beans
AS SELECT * FROM beans WHERE delicious = true;
    """)
generate_query("acls_lab")

# COMMAND ----------

# MAGIC %md
# MAGIC Steps:
# MAGIC * Run the cell above
# MAGIC * Copy the entire output to your clipboard
# MAGIC * Navigate to the Databricks SQL workspace
# MAGIC * Make sure that a DBSQL endpoint is running
# MAGIC * Use the left sidebar to select the **SQL Editor**
# MAGIC * Paste the query above and click the blue **Run** in the top right
# MAGIC 
# MAGIC **NOTE**: You will need to be connected to a DBSQL endpoint to execute these queries successfully. If you cannot connect to a DBSQL endpoint, you will need to contact your administrator to give you access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Your Database
# MAGIC In the Data Explorer, find the database you created earlier (this should follow the pattern `dbacademy_<username>_acls_lab`).
# MAGIC 
# MAGIC Clicking on the database name should display a list of the contained tables and views on the left hand side. On the right, you'll see some details about the database, including the **Owner** and **Location**.
# MAGIC 
# MAGIC Click the **Permissions** tab to review who presently has permissions (depending on your workspace configuration, some permissions may have been inherited from settings on the catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Database Permissions
# MAGIC 
# MAGIC Step:
# MAGIC 1. Make sure you have the **Permissions** tab selected for the database
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **USAGE**, **SELECT**, and **READ_METADATA** options
# MAGIC 1. Enter the username of your partner in the field at the top.
# MAGIC 1. Click **OK**
# MAGIC 
# MAGIC Confirm with your partner that you can each see each others' databases and tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a Query to Confirm
# MAGIC 
# MAGIC By granting `USAGE`, `SELECT`, and `READ_METADATA` on your database, your partner should now be able to freely query the tables and views in this database, but will not be able to create new tables OR modify your data.
# MAGIC 
# MAGIC In the SQL Editor, each user should run a series of queries to confirm this behavior in the database they were just added to.
# MAGIC 
# MAGIC **Make sure you specify your partner's database while running the queries below.**
# MAGIC 
# MAGIC Queries to execute:
# MAGIC * `SELECT * FROM <database>.beans`
# MAGIC * `SELECT * FROM <database>.tasty_beans`
# MAGIC * `SELECT * FROM <database>.beans MINUS SELECT * FROM <database>.tasty_beans`
# MAGIC * ```
# MAGIC UPDATE <database>.beans
# MAGIC SET color = 'pink'
# MAGIC WHERE name = 'black'
# MAGIC ```
# MAGIC 
# MAGIC **NOTE**: These first 3 queries should succeed, but the last should fail.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute a Query to Generate the Union of Your Beans
# MAGIC 
# MAGIC Modify the query below to specify the `beans` tables in each of your databases.
# MAGIC 
# MAGIC ```
# MAGIC SELECT * FROM <database>.beans
# MAGIC UNION ALL TABLE <database>.beans
# MAGIC ```
# MAGIC 
# MAGIC **NOTE**: Because random values were inserted for the `grams` and `delicious` columns, you should see 2 distinct rows for each `name`, `color` pair.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a Derivative View to Your Database
# MAGIC 
# MAGIC Modify the query below to register the results of the previous query to your database.
# MAGIC 
# MAGIC ```
# MAGIC CREATE VIEW <database>.our_beans AS
# MAGIC   SELECT * FROM <database>.beans
# MAGIC   UNION ALL TABLE <database>.beans
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Partner's View
# MAGIC 
# MAGIC Once your partner has successfully completed the previous step, run the following query against each of your tables; you should get the same results:
# MAGIC 
# MAGIC ```
# MAGIC SELECT name, color, delicious, sum(grams)
# MAGIC FROM our_beans
# MAGIC GROUP BY name, color, delicious
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Modify Permissions
# MAGIC 
# MAGIC Now try to drop each other's `beans` tables. At the moment, this shouldn't work.
# MAGIC 
# MAGIC Using the Data Explorer, add the `MODIFY` permission for your `beans` table for your partner.
# MAGIC 
# MAGIC Again, attempt to drop your partner's `beans` table. This time, it should succeed.
# MAGIC 
# MAGIC Try to re-execute queries against any of the views of tables you'd previously queried in this lab.
# MAGIC 
# MAGIC **NOTE**: If steps were completed successfully, none of your previous queries should work, as the data referenced by your views has been permanently deleted. This demonstrates the risks associated with providing `MODIFY` privileges to users on data that will be used in production applications and dashboards.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
