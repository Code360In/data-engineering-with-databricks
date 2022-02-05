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
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC 
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook as a triggered standalone job, and then add a dependent job using a DLT pipeline. 
# MAGIC 
# MAGIC 
# MAGIC By the end of this lesson, you should feel confident:
# MAGIC * Scheduling a notebook as a Databricks Job
# MAGIC * Describing job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Scheduling a DLT pipeline as a Databricks Job
# MAGIC * Configuring linear dependencies between tasks using the Databricks Jobs UI
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Here, we'll start by scheduling the notebook **`1 - Reset`**.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **`Create Job`** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **`reset`** for the task name
# MAGIC     1. Select the notebook **`1 - Reset`** using the notebook picker
# MAGIC     1. Select an Existing All Purpose Cluster from the **Cluster** dropdown
# MAGIC     1. Click **Create**
# MAGIC 
# MAGIC **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC 
# MAGIC Click the blue **Run now** button in the top right to start the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Run
# MAGIC 
# MAGIC As currently scheduled, our single notebook provides identical performance to the legacy Databricks Jobs UI, which only allowed a single notebook to be scheduled.
# MAGIC 
# MAGIC From the **Runs** tab, clicking on the start time field will display a preview of the notebook with results. If the job is still running, this will be under **Active Runs**, and the displayed notebook will occasionally update to show progress throughout execution. If it has already completed, it will be under **Completed Runs** and just display the static results of the executed notebook.
# MAGIC 
# MAGIC The notebook scheduled using the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC 
# MAGIC The actual outcome of the scheduled notebook is to reset the output of the DLT pipeline configured earlier in the course, as well as to print out the necessary variables used to configure this pipeline for users that may not have coded along previously.
# MAGIC 
# MAGIC Before continuing to the next step, make sure you either have access to a 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chron Scheduling of Databricks Jobs
# MAGIC 
# MAGIC Note that on the right hand side of the Jobs UI, directly under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 
# MAGIC Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 
# MAGIC Changing the **Schedule type** field from **Manual** to **Scheduled** will bring up a chron scheduling UI.
# MAGIC 
# MAGIC This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in chron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 
# MAGIC At this time, we'll leave our job set with **Manual** scheduling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured in the previous step.
# MAGIC 
# MAGIC **NOTE**: This step assumes that the DLT pipeline describe in the lab for module 3 of this course was configured successfully. If this is not the case, instructions are included for configuring this DLT pipeline in the run output of the **`reset`** notebook executed above.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar
# MAGIC 1. Select the job you defined above by clicking on the name
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **`dlt`**
# MAGIC     1. From **Type**, select **`Pipeline`**
# MAGIC     1. Click the **Pipeline** field and select the DLT pipeline you configured previously
# MAGIC     1. Note that the **Depends on** field defaults to your previously defined task
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. Your **`reset`** task will be at the top, leading into your **`dlt`** task. This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Click **Run now** to execute your job.
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your DLT pipeline is deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Multi-Task Run Results
# MAGIC 
# MAGIC Clicking into the job run will replicate the UI showing both tasks. The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failure occur. Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC 
# MAGIC You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps; note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC 
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
