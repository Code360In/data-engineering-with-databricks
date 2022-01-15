# Databricks notebook source
# MAGIC %run ../../Includes/multi-hop-setup

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC **NOTE**: This lab is configured to work with the DLT pipeline completed as part of the DLT lab in the previous module. If you have not successfully completed this lab, follow the instructions below to configure a pipeline using specified notebook.
# MAGIC 
# MAGIC Instructions for configuring DLT pipeline:
# MAGIC 1. Click the **Jobs** button on the sidebar, then select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** of your choosing.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook `4.1.2 - DLT Job`.
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

