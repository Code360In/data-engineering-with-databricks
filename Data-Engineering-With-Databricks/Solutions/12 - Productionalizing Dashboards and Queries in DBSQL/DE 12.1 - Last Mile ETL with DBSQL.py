# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Last Mile ETL with Databricks SQL
# MAGIC 
# MAGIC Before we continue, let's do a recap of some of the things we've learned so far:
# MAGIC 1. The Databricks workspace contains a suite of tools to simplify the data engineering development lifecycle
# MAGIC 1. Databricks notebooks allow users to mix SQL with other programming languages to define ETL workloads
# MAGIC 1. Delta Lake provides ACID compliant transactions and makes incremental data processing easy in the Lakehouse
# MAGIC 1. Delta Live Tables extends the SQL syntax to support many design patterns in the Lakehouse, and simplifies infrastructure deployment
# MAGIC 1. Multi-task jobs allows for full task orchestration, adding dependencies while scheduling a mix of notebooks and DLT pipelines
# MAGIC 1. Databricks SQL allows users to edit and execute SQL queries, build visualizations, and define dashboards
# MAGIC 1. Data Explorer simplifies managing Table ACLs, making Lakehouse data available to SQL analysts (soon to be expanded greatly by Unity Catalog)
# MAGIC 
# MAGIC In this section, we'll focus on exploring more DBSQL functionality to support production workloads. 
# MAGIC 
# MAGIC We'll start by focusing on leveraging Databricks SQL to configure queries that support last mile ETL for analytics. Note that while we'll be using the Databricks SQL UI for this demo, SQL Endpoints <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">integrate with a number of other tools to allow external query execution</a>, as well as having <a href="https://docs.databricks.com/sql/api/index.html" target="_blank">full API support for executing arbitrary queries programmatically</a>.
# MAGIC 
# MAGIC From these query results, we'll generate a series of visualizations, which we'll combine into a dashboard.
# MAGIC 
# MAGIC Finally, we'll walk through scheduling updates for queries and dashboards, and demonstrate setting alerts to help monitor the state of production datasets over time.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you will feel confident:
# MAGIC * Using Databricks SQL as a tool to support production ETL tasks backing analytic workloads
# MAGIC * Configuring SQL queries and visualizations with the Databricks SQL Editor
# MAGIC * Creating dashboards in Databricks SQL
# MAGIC * Scheduling updates for queries and dashboards
# MAGIC * Setting alerts for SQL queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup Script
# MAGIC The following cells runs a notebook that defines a class we'll use to generate SQL queries.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-12.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Demo Database
# MAGIC Execute the following cell and copy the results into the Databricks SQL Editor.
# MAGIC 
# MAGIC These queries:
# MAGIC * Create a new database
# MAGIC * Declare two tables (we'll use these for loading data)
# MAGIC * Declare two functions (we'll use these for generating data)
# MAGIC 
# MAGIC Once copied, execute the query using the **Run** button.

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**: The queries above are only designed to be run once after resetting the demo completely to reconfigure the environment. Users will need to have **`CREATE`** and **`USAGE`** permissions on the catalog to execute them.

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 
# MAGIC **WARNING:** Make sure to select your database before proceeding as the **`USE`** statement<br/>doesn't yet change the database against which your queries will execute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Query to Load Data
# MAGIC Execute the cell below to print out a formatted SQL query for loading data in the **`user_ping`** table created in the previous step.
# MAGIC 
# MAGIC Save this query with the name **Load Ping Data**.
# MAGIC 
# MAGIC Run this query to load a batch of data.

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# MAGIC %md
# MAGIC Executing the query should load some data and return a preview of the data in the table.
# MAGIC 
# MAGIC **NOTE**: Random numbers are being used to define and load data, so each user will have slightly different values present.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set a Query Refresh Schedule
# MAGIC 
# MAGIC Steps:
# MAGIC * Locate the **Refresh Schedule** field at the bottom right of the SQL query editor box; click the blue **Never**
# MAGIC * Use the drop down to change to Refresh every **1 minute**
# MAGIC * For **Ends**, click the **On** radio button
# MAGIC * Select tomorrow's date
# MAGIC * Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Query to Track Total Records
# MAGIC 
# MAGIC Execute the cell below to print out a formatted SQL query to track total records in the **`user_ping`** table.
# MAGIC 
# MAGIC Save this query with the name **User Counts**.
# MAGIC 
# MAGIC Run the query to calculate the current results.

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Bar Graph Visualization
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the **Add Visualization** button
# MAGIC * Click on the name (should default to something like **`Visualization 1`**) and change the name to **Total User Records**
# MAGIC * Set **`user_id`** for the **X Column**
# MAGIC * Set **`total_records`** for the **Y Columns**
# MAGIC * Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a New Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC * Click the **Create new dashboard** option
# MAGIC * Name your dashboard <strong>User Ping Summary **`<your_initials_here>`**</strong>
# MAGIC * Click **Save** to create the new dashboard
# MAGIC * Your newly created dashboard should now be selected as the target; click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Query to Calculate the Recent Average Ping
# MAGIC 
# MAGIC Execute the cell below to print out a formatted SQL query to calculate the average ping observed per user over a 3 minute window.
# MAGIC 
# MAGIC Save this query with the name **Avg Ping**.
# MAGIC 
# MAGIC Run the query to calculate the current results.

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Line Plot Visualization to your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the **Add Visualization** button
# MAGIC * Click on the name (should default to something like **`Visualization 1`**) and change the name to **Avg User Ping**
# MAGIC * Select **`Line`** for the **Visualization Type**
# MAGIC * Set **`end_time`** for the **X Column**
# MAGIC * Set **`avg_ping`** for the **Y Columns**
# MAGIC * Set **`user_id`** for the **Group by**
# MAGIC * Click **Save**
# MAGIC * Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC * Select the dashboard you created earlier
# MAGIC * Click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Query to Report Summary Statistics
# MAGIC 
# MAGIC Execute the cell below to print out a formatted SQL query that summarizes all records for a user.
# MAGIC 
# MAGIC Save this query with the name **Ping Summary**.
# MAGIC 
# MAGIC Run the query to calculate the current results.

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add the Summary Table to your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC * Select the dashboard you created earlier
# MAGIC * Click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review and Refresh your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC * Use the left side bar to navigate to **Dashboards**
# MAGIC * Find the dashboard you've added your queries to
# MAGIC * Click the blue **Refresh** button to update your dashboard
# MAGIC * Click the **Schedule** button to review dashboard scheduling options
# MAGIC   * Note that scheduling a dashboard to update will execute all queries associated with that dashboard
# MAGIC   * Do not schedule the dashboard at this time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Share your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC * Click the blue **Share** button
# MAGIC * Select **All Users** from the top field
# MAGIC * Choose **Can Run** from the right field
# MAGIC * Click **Add**
# MAGIC * Change the **Credentials** to **Run as viewer**
# MAGIC 
# MAGIC **NOTE**: At present, no other users should have any permissions to run your dashboard, as they have not been granted permissions to the underlying databases and tables using Table ACLs. If you wish other users to be able to trigger updates to your dashboard, you will either need to grant them permissions to **Run as owner** or add permissions for the tables referenced in your queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up an Alert
# MAGIC 
# MAGIC Steps:
# MAGIC * Use the left side bar to navigate to **Alerts**
# MAGIC * Click **Create Alert** in the top right
# MAGIC * Click the field at the top left of the screen to give the alert a name **`<your_initials> Count Check`**
# MAGIC * Select your **User Counts** query
# MAGIC * For the **Trigger when** options, configure:
# MAGIC   * **Value column**: **`total_records`**
# MAGIC   * **Condition**: **`>`**
# MAGIC   * **Threshold**: **`15`**
# MAGIC * For **Refresh**, select **Never**
# MAGIC * Click **Create Alert**
# MAGIC * On the next screen, click the blue **Refresh** in the top right to evaluate the alert

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Alert Destination Options
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC * From the preview of your alert, click the blue **Add** button to the right of **Destinations** on the right side of the screen
# MAGIC * At the bottom of the window that pops up, locate the and click the blue text in the message **Create new destinations in Alert Destinations**
# MAGIC * Review the available alerting options

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
