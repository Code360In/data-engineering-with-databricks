# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC In this lab, you'll be configuring a multi-task job comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries the gold table produced by this pipeline as well as various metrics output by DLT
# MAGIC 
# MAGIC By the end of this lab, you should feel confident:
# MAGIC * Scheduling a notebook as a Databricks Job
# MAGIC * Scheduling a DLT pipeline as a Databricks Job
# MAGIC * Configuring linear dependencies between tasks using the Databricks Jobs UI
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Here, we'll start by scheduling the notebook **1 - Batch Job**.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Batch-Job** for the task name
# MAGIC     1. Select the notebook **1 - Batch Job** using the notebook picker
# MAGIC     1. Select an Existing All Purpose Cluster from the **Cluster** dropdown
# MAGIC     1. Click **Create**
# MAGIC 
# MAGIC **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC 
# MAGIC Click the blue **Run now** button in the top right to confirm that you have successfully configured this task. From the **Runs** tab, clicking on the start time field will pull up the notebook with results.
# MAGIC 
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured in the previous step.
# MAGIC 
# MAGIC **NOTE**: This step assumes that the DLT pipeline describe in the lab for module 3 of this course was configured successfully. If this is not the case, instructions are included for configuring this DLT pipeline in the run output of the **Batch-Job** notebook executed above.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar
# MAGIC 1. Select the job you defined above by clicking on the name (this should have the name **Batch-Job**)
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **DLT-Pipeline**
# MAGIC     1. From **Type**, select **Pipeline**
# MAGIC     1. Click the **Pipeline** field and select the DLT pipeline you configured previously
# MAGIC     1. Note that the **Depends on** field defaults to your previously defined task
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. Your **Batch-Job** task will be at the top, leading into your **DLT-Pipeline** task. This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Before clicking **Run now**, click the job name in the top left and provide something unique and descriptive, like **<your_initials>-MTJ-lab**
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your DLT pipeline is deployed. Feel free to skip clicking **Run now** until the next task is configured if you don't want to wait.
# MAGIC 
# MAGIC ## Schedule an Additional Notebook Task
# MAGIC 
# MAGIC An additional notebook has been provided which queries some of the DLT metrics and the gold table defined in the DLT pipeline. We'll add this as a final task in our job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the **Tasks** tab of the job you've been configuring
# MAGIC 1. Click the blue **+** button to add another task
# MAGIC     1. Specify the **Task name** as **Query-Results**
# MAGIC     1. Leave the **Type** set to **Notebook**
# MAGIC     1. Select the notebook **3 - Query Results Job** using the notebook picker
# MAGIC     1. Note that the **Depends on** field defaults to your previously defined task
# MAGIC     1. Select an Existing All Purpose Cluster from the **Cluster** dropdown
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC Click the blue **Run now** button in the top right of the screen to run this job.
# MAGIC 
# MAGIC From the **Runs** tab, you will be able to click on the start time for this run under the **Active runs** section and visually track task progress.
# MAGIC 
# MAGIC Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
