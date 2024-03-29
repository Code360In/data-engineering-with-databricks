# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Migrating SQL Notebooks to Delta Live Tables
# MAGIC 
# MAGIC This notebook dictates an overall structure for the lab exercise, configures the environment for the lab, provides simulated data streaming, and performs cleanup once you are done. A notebook like this is not typically needed in a production pipeline scenario.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Convert existing data pipelines to Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Begin by running the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run "../../Includes/dlt-setup" $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. You will re-run this command to land additional data later.

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar, then select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** of your choosing.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook `3.3.4 - LAB - Migrating a SQL Pipeline to DLT`.
# MAGIC 1. Run the cell below to generate values for **source**, **Target** and **Storage Location**. (All of these will include your current username).
# MAGIC    * Click `Add configuration`; enter the word `source` in the **Key** field and the output printed next to `source` below in the value field.
# MAGIC    * Enter the database name printed next to `Target` below in the **Target** field.
# MAGIC    * Enter the location printed next to `Storage Location` below in the **Storage Location** field.
# MAGIC 1. Set **Pipeline Mode** to **Triggered**.
# MAGIC 1. Disable autoscaling.
# MAGIC 1. Set the number of wokers to 1.
# MAGIC 1. Click **Create**.

# COMMAND ----------

storage_location = userhome + "/output"
print(f"source : {dataLandingLocation.split(':')[1]}")
print(f"Target: {database}")
print(f"Storage Location: {storage_location.split(':')[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run your Pipeline
# MAGIC 
# MAGIC Select **Development** mode, which accelerates the development lifecycle by reusing the same cluster across runs. It will also turn off automatic retries when jobs fail.
# MAGIC 
# MAGIC Click **Start** to begin the first update to your table.
# MAGIC 
# MAGIC Delta Live Tables will automatically deploy all the necessary infrastructure and resolve the dependencies between all datasets.
# MAGIC 
# MAGIC **NOTE**: The first table update make take several minutes as relationships are resolved and infrastructure deploys.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open and Complete DLT Pipeline Notebook
# MAGIC 
# MAGIC You will perform your work in this <a href="$./3.3.4 - LAB - Migrating a SQL Pipeline to DLT" target="_blank">companion Notebook</a>, which you will ultimately deploy as a pipeline.
# MAGIC 
# MAGIC Open the Notebook and, following the guidelines provided therein, fill in the cells where prompted to implement a multi-hop architecture similar to the one we worked with in the previous section.
# MAGIC 
# MAGIC **NOTE**: As a first step to preparing your pipeline, run the following cell to obtain the cloud file location. Substitue this value for the text that reads `<CLOUD_FILES LOCATION>`. This value will be unique within the workspace to your user identity to prevent possible interference between users within the same workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Code in Development Mode
# MAGIC 
# MAGIC Don't despair if your pipeline fails the first time. Delta Live Tables is in active development, and error messages are improving all the time.
# MAGIC 
# MAGIC Because relationships between tables are mapped as a DAG, error messages will often indicate that a dataset isn't found.
# MAGIC 
# MAGIC Let's consider our DAG below:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/dlt_dag.png" width="400">
# MAGIC 
# MAGIC If the error message `Dataset not found: 'recordings_parsed'` is raised, there may be several culprits:
# MAGIC 1. The logic defining `recordings_parsed` is invalid
# MAGIC 1. There is an error reading from `recordings_bronze`
# MAGIC 1. A typo exists in either `recordings_parsed` or `recordings_bronze`
# MAGIC 
# MAGIC The safest way to identify the culprit is to iteratively add table/view definitions back into your DAG starting from your initial ingestion tables. You can simply comment out later table/view definitions and uncomment these between runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Results
# MAGIC 
# MAGIC Assuming your pipeline runs successfully, display the contents of the gold table.
# MAGIC 
# MAGIC **NOTE**: Because we specified a value for **Target**, tables are published to the specified database. Without a **Target** specification, we would need to query the table based on its underlying location in DBFS (relative to the **Storage Location**).

# COMMAND ----------

spark.sql(f"SELECT * FROM {database}daily_patient_avg")

# COMMAND ----------

# MAGIC %md
# MAGIC Trigger another file arrival with the following cell. Feel free to run it a couple more times if desired. Following this, run the pipeline again and view the results. Feel free to re-run the cell above to gain an updated view of the `daily_patient_avg` table.

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Ensure that you delete your pipeline from the DLT UI, and run the following cell to clean up the files and tables that were created as part of the lab setup and execution.

# COMMAND ----------

# MAGIC %run "../../Includes/dlt-setup" $mode="clean"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lab, you learned to convert an existing data pipeline to a Delta Live Tables SQL pipeline, and deployed that pipeline using the DLT UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Delta Live Tables Documentation</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Delta Live Tables Demo</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
