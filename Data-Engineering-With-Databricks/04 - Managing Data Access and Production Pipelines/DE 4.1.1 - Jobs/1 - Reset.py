# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../../Includes/classroom-setup-dlt-demo

# COMMAND ----------

# MAGIC %md
# MAGIC If you have not previously configured this DLT pipeline successfully, the following cell prints out two values that will be used during the configuration steps that follow.

# COMMAND ----------

print(f"Target:           {DA.db_name}")
print(f"Storage location: {DA.paths.storage_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and configure a pipeline
# MAGIC 
# MAGIC The instructions below refer to the same pipeline created during the previous code along for DLT; if you successfully configured this notebook previously, you should not need to reconfigure this pipeline now.
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar,
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** of your choosing.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the companion notebook called **2 - DLT Job**.   
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above. (This should follow the pattern **`dbacademy_<username>_dlt_demo`**)
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC 1. Uncheck the **Enable autoscaling** box, and set the number of workers to 1.,
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
