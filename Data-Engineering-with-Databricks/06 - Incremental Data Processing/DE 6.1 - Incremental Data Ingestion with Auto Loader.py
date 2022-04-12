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
# MAGIC # Incremental Data Ingestion with Auto Loader
# MAGIC 
# MAGIC Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data reduces redundant processing and helps enterprises reliably scale data pipelines.
# MAGIC 
# MAGIC The first step for any successful data lakehouse implementation is ingesting into a Delta Lake table from cloud storage. 
# MAGIC 
# MAGIC Historically, ingesting files from a data lake into a database has been a complicated process.
# MAGIC 
# MAGIC Databricks Auto Loader provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage. In this notebook, you'll see Auto Loader in action.
# MAGIC 
# MAGIC Due to the benefits and scalability that Auto Loader delivers, Databricks recommends its use as general **best practice** when ingesting data from cloud object storage.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Execute Auto Loader code to incrementally ingest data from cloud storage to Delta Lake
# MAGIC * Describe what happens when a new file arrives in a directory configured for Auto Loader
# MAGIC * Query a table fed by a streaming Auto Loader query
# MAGIC 
# MAGIC ## Dataset Used
# MAGIC This demo uses simplified artificially generated medical data representing heart rate recordings delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to reset the demo and configure required variables and help functions.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Using Auto Loader
# MAGIC 
# MAGIC In the cell below, a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.
# MAGIC 
# MAGIC The following notebook will provide a more robust overview of Structured Streaming. If you wish to learn more about Auto Loader options, refer to the <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentation</a>.
# MAGIC 
# MAGIC Note that when using Auto Loader with automatic <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">schema inference and evolution</a>, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below.
# MAGIC 
# MAGIC | argument | what it is | how it's used |
# MAGIC | --- | --- | --- |
# MAGIC | **`data_source`** | The directory of the source data | Auto Loader will detect new files as they arrive in this location and queue them for ingestion; passed to the **`.load()`** method |
# MAGIC | **`source_format`** | The format of the source data |  While the format for all Auto Loader queries will be **`cloudFiles`**, the format of the source data should always be specified for the **`cloudFiles.format`** option |
# MAGIC | **`table_name`** | The name of the target table | Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the **`.table()`** method. Note that you can either append to an existing table or create a new table |
# MAGIC | **`checkpoint_directory`** | The location for storing metadata about the stream | This argument is pass to the **`checkpointLocation`** and **`cloudFiles.schemaLocation`** options. Checkpoints keep track of streaming progress, while the schema location tracks updates to the fields in the source dataset |
# MAGIC 
# MAGIC **NOTE**: The code below has been streamlined to demonstrate Auto Loader functionality. We'll see in later lessons that additional transformations can be applied to source data before saving them to Delta Lake.

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC In the following cell, we use the previously defined function and some path variables defined in the setup script to begin an Auto Loader stream.
# MAGIC 
# MAGIC Here, we're reading from a source directory of JSON files.

# COMMAND ----------

query = autoload_to_table(data_source = f"{DA.paths.working_dir}/tracker",
                          source_format = "json",
                          table_name = "target_table",
                          checkpoint_directory = f"{DA.paths.checkpoints}/target_table")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Because Auto Loader uses Spark Structured Streaming to load data incrementally, the code above doesn't appear to finish executing.
# MAGIC 
# MAGIC We can think of this as a **continuously active query**. This means that as soon as new data arrives in our data source, it will be processed through our logic and loaded into our target table. We'll explore this in just a second.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Helper Function for Streaming Lessons
# MAGIC 
# MAGIC Our notebook-based lessons combine streaming functions with batch and streaming queries against the results of those operations. These notebooks are for instructional purposes and intended for interactive, cell-by-cell execution. This pattern is not intended for production.
# MAGIC 
# MAGIC Below, we define a helper function that prevents our notebook from executing the next cell just long enough to ensure data has been written out by a given streaming query. This code should not be necessary in a production job.

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Query Target Table
# MAGIC 
# MAGIC Once data has been ingested to Delta Lake with Auto Loader, users can interact with it the same way they would any table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note that the **`_rescued_data`** column is added by Auto Loader automatically to capture any data that might be malformed and not fit into the table otherwise.
# MAGIC 
# MAGIC While Auto Loader captured the field names for our data correctly, note that it encoded all fields as **`STRING`** type. Because JSON is a text-based format, this is the safest and most permissive type, ensuring that the least amount of data is dropped or ignored at ingestion due to type mismatch.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Use the cell below to define a temporary view that summarizes the recordings in our target table.
# MAGIC 
# MAGIC We'll use this view below to demonstrate how new data is automatically ingested with Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts AS
# MAGIC   SELECT device_id, count(*) total_recordings
# MAGIC   FROM target_table
# MAGIC   GROUP BY device_id;
# MAGIC   
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC As mentioned previously, Auto Loader is configured to incrementally process files from a directory in cloud object storage into a Delta Lake table.
# MAGIC 
# MAGIC We have configured and are currently executing a query to process JSON files from the location specified by **`source_path`** into a table named **`target_table`**. Let's review the contents of the **`source_path`** directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC At present, you should see a single JSON file listed in this location.
# MAGIC 
# MAGIC The method in the cell below was configured in our setup script to allow us to model an external system writing data to this directory. Each time you execute the cell below, a new file will land in the **`source_path`** directory.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC List the contents of the **`source_path`** again using the cell below. You should see an additional JSON file for each time you ran the previous cell.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Tracking Ingestion Progress
# MAGIC 
# MAGIC Historically, many systems have been configured to either reprocess all records in a source directory to calculate current results or require data engineers to implement custom logic to identify new data that's arrived since the last time a table was updated.
# MAGIC 
# MAGIC With Auto Loader, your table has already been updated.
# MAGIC 
# MAGIC Run the query below to confirm that new data has been ingested.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The Auto Loader query we configured earlier automatically detects and processes records from the source directory into the target table. There is a slight delay as records are ingested, but an Auto Loader query executing with default streaming configuration should update results in near real time.
# MAGIC 
# MAGIC The query below shows the table history. A new table version should be indicated for each **`STREAMING UPDATE`**. These update events coincide with new batches of data arriving at the source.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Clean Up
# MAGIC Feel free to continue landing new data and exploring the table results with the cells above.
# MAGIC 
# MAGIC When you're finished, run the following cell to stop all active streams and remove created resources before continuing.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
