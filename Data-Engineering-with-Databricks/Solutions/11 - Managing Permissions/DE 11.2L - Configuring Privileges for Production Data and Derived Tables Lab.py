# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Configuring Privileges for Production Data and Derived Tables
# MAGIC 
# MAGIC The instructions as detailed below are provided for pairs of users to explore how Table ACLs on Databricks work. It leverages Databricks SQL and the Data Explorer to accomplish these tasks, and assumes that neither user has admin privileges for the workspace. An admin will need to have previously granted **`CREATE`** and **`USAGE`** privileges on a catalog for users to be able to create databases in Databricks SQL.
# MAGIC 
# MAGIC ##Learning Objectives
# MAGIC 
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Use Data Explorer to navigate relational entities
# MAGIC * Configure permissions for tables and views with Data Explorer
# MAGIC * Configure minimal permissions to allow for table discovery and querying
# MAGIC * Change ownership for databases, tables, and views created in DBSQL

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.2L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Exchange User Names with your Partner
# MAGIC If you are not in a workspace where your usernames correspond with your email address, make sure your partner has your username.
# MAGIC 
# MAGIC They will need this when assigning privileges and searching for your database at later steps.
# MAGIC 
# MAGIC The following cell will print your username.

# COMMAND ----------

print(f"Your username: {DA.username}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Generate Setup Statements
# MAGIC 
# MAGIC The following cell uses Python to extract the username of the current user and format this into several statements used to create databases, tables, and views.
# MAGIC 
# MAGIC Both students should execute the following cell. 
# MAGIC 
# MAGIC Successful execution will print out a series of formatted SQL queries, which can be copied into the DBSQL query editor and executed.

# COMMAND ----------

DA.generate_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Run the cell above
# MAGIC 1. Copy the entire output to your clipboard
# MAGIC 1. Navigate to the Databricks SQL workspace
# MAGIC 1. Make sure that a DBSQL endpoint is running
# MAGIC 1. Use the left sidebar to select the **SQL Editor**
# MAGIC 1. Paste the query above and click the blue **Run** in the top right
# MAGIC 
# MAGIC **NOTE**: You will need to be connected to a DBSQL endpoint to execute these queries successfully. If you cannot connect to a DBSQL endpoint, you will need to contact your administrator to give you access.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Find Your Database
# MAGIC In the Data Explorer, find the database you created earlier (this should follow the pattern **`dbacademy_<username>_dewd_acls_lab`**).
# MAGIC 
# MAGIC Clicking on the database name should display a list of the contained tables and views on the left hand side.
# MAGIC 
# MAGIC On the right, you'll see some details about the database, including the **Owner** and **Location**.
# MAGIC 
# MAGIC Click the **Permissions** tab to review who presently has permissions (depending on your workspace configuration, some permissions may have been inherited from settings on the catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Change Database Permissions
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Make sure you have the **Permissions** tab selected for the database
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **USAGE**, **SELECT**, and **READ_METADATA** options
# MAGIC 1. Enter the username of your partner in the field at the top.
# MAGIC 1. Click **OK**
# MAGIC 
# MAGIC Confirm with your partner that you can each see each others' databases and tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run a Query to Confirm
# MAGIC 
# MAGIC By granting **`USAGE`**, **`SELECT`**, and **`READ_METADATA`** on your database, your partner should now be able to freely query the tables and views in this database, but will not be able to create new tables OR modify your data.
# MAGIC 
# MAGIC In the SQL Editor, each user should run a series of queries to confirm this behavior in the database they were just added to.
# MAGIC 
# MAGIC **Make sure you specify your partner's database while running the queries below.**
# MAGIC 
# MAGIC **NOTE**: These first 3 queries should succeed, but the last should fail.

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_confirmation_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Execute a Query to Generate the Union of Your Beans
# MAGIC 
# MAGIC Execute the query below against your own databases.
# MAGIC 
# MAGIC **NOTE**: Because random values were inserted for the **`grams`** and **`delicious`** columns, you should see 2 distinct rows for each **`name`**, **`color`** pair.

# COMMAND ----------

DA.generate_union_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Register a Derivative View to Your Database
# MAGIC 
# MAGIC Execute the query below to register the results of the previous query to your database.

# COMMAND ----------

DA.generate_derivative_view()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Query Your Partner's View
# MAGIC 
# MAGIC Once your partner has successfully completed the previous step, run the following query against each of your tables; you should get the same results:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_partner_view("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Add Modify Permissions
# MAGIC 
# MAGIC Now try to drop each other's **`beans`** tables. 
# MAGIC 
# MAGIC At the moment, this shouldn't work.
# MAGIC 
# MAGIC Using the Data Explorer, add the **`MODIFY`** permission for your **`beans`** table for your partner.
# MAGIC 
# MAGIC Again, attempt to drop your partner's **`beans`** table. 
# MAGIC 
# MAGIC It should again fail. 
# MAGIC 
# MAGIC **Only the owner of a table should be able to issue this statement**.<br/>
# MAGIC (Note that ownership can be transferred from an individual to a group, if desired).
# MAGIC 
# MAGIC Instead, execute a query to delete records from your partner's table:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_delete_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC This query should successfully drop all records from the target table.
# MAGIC 
# MAGIC Try to re-execute queries against any of the views of tables you'd previously queried in this lab.
# MAGIC 
# MAGIC **NOTE**: If steps were completed successfully, none of your previous queries should return results, as the data referenced by your views has been deleted. This demonstrates the risks associated with providing **`MODIFY`** privileges to users on data that will be used in production applications and dashboards.
# MAGIC 
# MAGIC If you have additional time, see if you can use the Delta methods **`DESCRIBE HISTORY`** and **`RESTORE`** to revert the records in your table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
