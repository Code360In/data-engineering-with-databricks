# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## End-to-End ETL in the Lakehouse
# MAGIC 
# MAGIC In this notebook, you will pull together concepts learned throughout the course to complete an example data pipeline.
# MAGIC 
# MAGIC The following is a non-exhaustive list of skills and tasks necessary to successfully complete this exercise:
# MAGIC * Using Databricks notebooks to write queries in SQL and Python
# MAGIC * Creating and modifying databases, tables, and views
# MAGIC * Using Auto Loader and Spark Structured Streaming for incremental data processing in a multi-hop architecture
# MAGIC * Using Delta Live Table SQL syntax
# MAGIC * Configuring a Delta Live Table pipeline for continuous processing
# MAGIC * Using Databricks Jobs to orchestrate tasks from notebooks stored in Repos
# MAGIC * Setting chronological scheduling for Databricks Jobs
# MAGIC * Defining queries in Databricks SQL
# MAGIC * Creating visualizations in Databricks SQL
# MAGIC * Defining Databricks SQL dashboards to review metrics and results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset all the databases and directories associated with this lab.

# COMMAND ----------

# MAGIC %run ../../Includes/classroom-setup-dlt-lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. 

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and Configure a DLT Pipeline
# MAGIC **NOTE**: The main difference between the instructions here and in previous labs with DLT is that in this instance, we will be setting up our pipeline for **Continuous** execution in **Production** mode.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar, then select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** of your choosing.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook **1 - DLT Task**.
# MAGIC 1. Run the cell below to generate values for **source**, **Target** and **Storage Location**. (All of these will include your current username).
# MAGIC    * Click **Add configuration**; enter the word **source** in the **Key** field and the output printed next to **source** below in the value field.
# MAGIC    * Enter the database name printed next to **Target** below in the **Target** field.
# MAGIC    * Enter the location printed next to **Storage Location** below in the **Storage Location** field.
# MAGIC 1. Set **Pipeline Mode** to **Continuous**.
# MAGIC 1. Disable autoscaling.
# MAGIC 1. Set the number of workers to 1.
# MAGIC 1. Click **Create**.
# MAGIC 
# MAGIC In the UI that populates, change from **Development** to **Production** mode. This should begin the deployment of infrastructure.

# COMMAND ----------

print(f"source:           {DA.paths.data_landing_location}")
print(f"Target:           {DA.db_name}")
print(f"Storage Location: {DA.paths.storage_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC Our DLT pipeline is setup to process data as soon as it arrives. We'll schedule a notebook to land a new batch of data each minute so we can see this functionality in action.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Land-Data** for the task name
# MAGIC     1. Select the notebook **2 - Land New Data** using the notebook picker
# MAGIC     1. Select an Existing All Purpose Cluster from the **Cluster** dropdown
# MAGIC     1. Click **Create**
# MAGIC 
# MAGIC **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC 
# MAGIC ## Set a Chronological Schedule for your Job
# MAGIC Steps:
# MAGIC * On the right hand side of the Jobs UI, locate **Schedule** section.
# MAGIC * Click on the **Edit schedule** button to explore scheduling options.
# MAGIC * Change **Schedule type** field from **Manual** to **Scheduled** will bring up a chron scheduling UI.
# MAGIC * Set the schedule to update every **2 minutes**
# MAGIC * Click **Save**
# MAGIC 
# MAGIC **NOTE**: If you wish, you can click **Run now** to trigger the first run, or wait until the top of the next minute to make sure your scheduling has worked successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register DLT Event Metrics for Querying with DBSQL
# MAGIC 
# MAGIC The following cell prints out SQL statements to register the DLT event logs to your target database for querying in DBSQL.
# MAGIC 
# MAGIC Execute the output code with the DBSQL Query Editor to register these tables and views. 
# MAGIC 
# MAGIC Explore each and make note of the logged event metrics.

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# SOURCE-ONLY
# Will use this later to refactor in testing
for command in generate_register_dlt_event_metrics_sql_string.split(";"):
    if len(command) > 0:
        print(command)
        print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Query on the Gold Table
# MAGIC 
# MAGIC The **daily_patient_avg** table is automatically updated each time a new batch of data is processed through the DLT pipeline. Each time a query is executed against this table, DBSQL will confirm if there is a newer version and then materialize results from the newest available version.
# MAGIC 
# MAGIC Run the following cell to print out a query with your database name. Save this as a DBSQL query.

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Line Plot Visualization
# MAGIC 
# MAGIC To track trends in patient averages over time, create a line plot and add it to a new dashboard.
# MAGIC 
# MAGIC Create a line plot with the following settings:
# MAGIC * **X Column**: **`date`**
# MAGIC * **Y Columns**: **`avg_heartrate`**
# MAGIC * **Group By**: **`name`**
# MAGIC 
# MAGIC Add this visualization to a dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Track Data Processing Progress
# MAGIC 
# MAGIC The code below extracts the **`flow_name`**, **`timestamp`**, and **`num_output_rows`** from the DLT event logs.
# MAGIC 
# MAGIC Save this query in DBSQL, then define a bar plot visualization that shows:
# MAGIC * **X Column**: **`timestamp`**
# MAGIC * **Y Columns**: **`num_output_rows`**
# MAGIC * **Group By**: **`flow_name`**
# MAGIC 
# MAGIC Add your visualization to your dashboard.

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh your Dashboard and Track Results
# MAGIC 
# MAGIC The **Land-Data** notebook scheduled with Jobs above has 12 batches of data, each representing a month of recordings for our small sampling of patients. As configured per our instructions, it should take just over 20 minutes for all of these batches of data to be triggered and processed (we scheduled the Databricks Job to run every 2 minutes, and batches of data will process through our pipeline very quickly after initial ingestion).
# MAGIC 
# MAGIC Refresh your dashboard and review your visualizations to see how many batches of data have been processed. (If you followed the instructions as outlined here, there should be 12 distinct flow updates tracked by your DLT metrics.) If all source data has not yet been processed, you can go back to the Databricks Jobs UI and manually trigger additional batches.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute a Query to Repair Broken Data
# MAGIC 
# MAGIC Review the code that defined the **`recordings_enriched`** table to identify the filter applied for the quality check.
# MAGIC 
# MAGIC In the cell below, write a query that returns all the records from the **`recordings_bronze`** table that were refused by this quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC For the purposes of our demo, let's assume that thorough manual review of our data and systems has demonstrated that occasionally otherwise valid heartrate recordings are returned as negative values.
# MAGIC 
# MAGIC Run the following query to examine these same rows with the negative sign removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC To complete our dataset, we wish to insert these fixed records into the silver **`recordings_enriched`** table.
# MAGIC 
# MAGIC Use the cell below to update the query used in the DLT pipeline to execute this repair.
# MAGIC 
# MAGIC **NOTE**: Make sure you update the code to only process those records that were previously rejected due to the quality check.

# COMMAND ----------

# TODO
# CREATE INCREMENTAL LIVE TABLE recordings_enriched
#   (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
# AS SELECT 
#   CAST(a.device_id AS INTEGER) device_id, 
#   CAST(a.mrn AS LONG) mrn, 
#   CAST(a.heartrate AS DOUBLE) heartrate, 
#   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
#   b.name
#   FROM STREAM(live.recordings_bronze) a
#   INNER JOIN STREAM(live.pii) b
#   ON a.mrn = b.mrn

# COMMAND ----------

# MAGIC %md
# MAGIC Use the cell below to manually or programmatically confirm that this update has been successful.
# MAGIC 
# MAGIC (The total number of records in the **`recordings_bronze`** should now be equal to the total records in **`recordings_enriched`**).

# COMMAND ----------

# TODO
<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consider Production Data Permissions
# MAGIC 
# MAGIC Note that while our manual repair of the data was successful, as the owner of these datasets, by default we have permissions to modify or delete these data from any location we're executing code.
# MAGIC 
# MAGIC To put this another way: our current permissions would allow us to change or drop our production tables permanently if an errant SQL query is accidentally executed with the current user's permissions (or if other users are granted similar permissions).
# MAGIC 
# MAGIC While for the purposes of this lab, we desired to have full permissions on our data, as we move code from development to production, it is safer to leverage <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">service principals</a> when scheduling Jobs and DLT Pipelines to avoid accidental data modifications.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shut Down Production Infrastructure
# MAGIC 
# MAGIC Note that Databricks Jobs, DLT Pipelines, and scheduled DBSQL queries and dashboards are all designed to provide sustained execution of production code. In this end-to-end demo, you were instructed to configure a Job and Pipeline for continuous data processing. To prevent these workloads from continuing to execute, you should **Pause** your Databricks Job and **Stop** your DLT pipeline. Deleting these assets will also ensure that production infrastructure is terminated.
# MAGIC 
# MAGIC **NOTE**: All instructions for DBSQL asset scheduling in previous lessons instructed users to set the update schedule to end tomorrow. You may choose to go back and also cancel these updates to prevent DBSQL endpoints from staying on until that time.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
