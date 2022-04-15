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
# MAGIC # Reasoning about Incremental Data
# MAGIC 
# MAGIC Spark Structured Streaming extends the functionality of Apache Spark to allow for simplified configuration and bookkeeping when processing incremental datasets. In the past, much of the emphasis for streaming with big data has focused on reducing latency to provide near real time analytic insights. While Structured Streaming provides exceptional performance in achieving these goals, this lesson will focus more on the applications of incremental data processing.
# MAGIC 
# MAGIC While incremental processing is not absolutely necessary to work successfully in the data lakehouse, our experience helping some of the world's largest companies derive insights from the world's largest datasets has led to the conclusion that many workloads can benefit substantially from an incremental processing approach. Many of the core features at the heart of Databricks have been optimized specifically to handle these ever-growing datasets.
# MAGIC 
# MAGIC Consider the following datasets and use cases:
# MAGIC * Data scientists need secure, de-identified, versioned access to frequently updated records in an operational database
# MAGIC * Credit card transactions need to be compared to past customer behavior to identify and flag fraud
# MAGIC * A multi-national retailer seeks to serve custom product recommendations using purchase history
# MAGIC * Log files from distributed systems need to be analayzed to detect and respond to instabilities
# MAGIC * Clickstream data from millions of online shoppers needs to be leveraged for A/B testing of UX
# MAGIC 
# MAGIC The above are just a small sample of datasets that grow incrementally and infinitely over time.
# MAGIC 
# MAGIC In this lesson, we'll explore the basics of working with Spark Structured Streaming to allow incremental processing of data. In the next lesson, we'll talk more about how this incremental processing model simplifies data processing in the data lakehouse.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the programming model used by Spark Structured Streaming
# MAGIC * Configure required options to perform a streaming read on a source
# MAGIC * Describe the requirements for end-to-end fault tolerance
# MAGIC * Configure required options to perform a streaming write to a sink

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Treating Infinite Data as a Table
# MAGIC 
# MAGIC The magic behind Spark Structured Streaming is that it allows users to interact with ever-growing data sources as if they were just a static table of records.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" width="800"/>
# MAGIC 
# MAGIC In the graphic above, a **data stream** describes any data source that grows over time. New data in a data stream might correspond to:
# MAGIC * A new JSON log file landing in cloud storage
# MAGIC * Updates to a database captured in a CDC feed
# MAGIC * Events queued in a pub/sub messaging feed
# MAGIC * A CSV file of sales closed the previous day
# MAGIC 
# MAGIC Many organizations have traditionally taken an approach of reprocessing the entire source dataset each time they want to update their results. Another approach would be to write custom logic to only capture those files or records that have been added since the last time an update was run.
# MAGIC 
# MAGIC Structured Streaming lets us define a query against the data source and automatically detect new records and propagate them through previously defined logic. 
# MAGIC 
# MAGIC **Spark Structured Streaming is optimized on Databricks to integrate closely with Delta Lake and Auto Loader.**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Basic Concepts
# MAGIC 
# MAGIC - The developer defines an **input table** by configuring a streaming read against a **source**. The syntax for doing this is similar to working with static data.
# MAGIC - A **query** is defined against the input table. Both the DataFrames API and Spark SQL can be used to easily define transformations and actions against the input table.
# MAGIC - This logical query on the input table generates the **results table**. The results table contains the incremental state information of the stream.
# MAGIC - The **output** of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Generally, a sink will be a durable system such as files or a pub/sub messaging bus.
# MAGIC - New rows are appended to the input table for each **trigger interval**. These new rows are essentially analogous to micro-batch transactions and will be automatically propagated through the results table to the sink.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" width="800"/>
# MAGIC 
# MAGIC 
# MAGIC For more information, see the analogous section in the <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts" target="_blank">Structured Streaming Programming Guide</a> (from which several images have been borrowed).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple of approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_ - that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The **`spark.readStream()`** method returns a **`DataStreamReader`** used to configure and query the stream.
# MAGIC 
# MAGIC In the previous lesson, we saw code configured for incrementally reading with Auto Loader. Here, we'll show how easy it is to incrementally read a Delta Lake table.
# MAGIC 
# MAGIC The code uses the PySpark API to incrementally read a Delta Lake table named **`bronze`** and register a streaming temp view named **`streaming_tmp_vw`**.
# MAGIC 
# MAGIC **NOTE**: A number of optional configurations (not shown here) can be set when configuring incremental reads, the most important of which allows you to <a href="https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate" target="_blank">limit the input rate</a>.

# COMMAND ----------

(spark.readStream
    .table("bronze")
    .createOrReplaceTempView("streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC When we execute a query on a streaming temporary view, we'll continue to update the results of the query as new data arrives in the source.
# MAGIC 
# MAGIC Think of a query executed against a streaming temp view as an **always-on incremental query**.
# MAGIC 
# MAGIC **NOTE**: Generally speaking, unless a human is actively monitoring the output of a query during development or live dashboarding, we won't return streaming results to a notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC You will recognize the data as being the same as the Delta table written out in our previous lesson.
# MAGIC 
# MAGIC Before continuing, click **`Stop Execution`** at the top of the notebook, **`Cancel`** immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Working with Streaming Data
# MAGIC We can execute most transformation against streaming temp views the same way we would with static data. Here, we'll run a simple aggregation to get counts of records for each **`device_id`**.
# MAGIC 
# MAGIC Because we are querying a streaming temp view, this becomes a streaming query that executes indefinitely, rather than completing after retrieving a single set of results. For streaming queries like this, Databricks Notebooks include interactive dashboards that allow users to monitor streaming performance. Explore this below.
# MAGIC 
# MAGIC One important note regarding this example: this is merely displaying an aggregation of input as seen by the stream. **None of these records are being persisted anywhere at this point.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(device_id) AS total_recordings
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY device_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Before continuing, click **`Stop Execution`** at the top of the notebook, **`Cancel`** immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Unsupported Operations
# MAGIC 
# MAGIC Most operations on a streaming DataFrame are identical to a static DataFrame. There are <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">some exceptions to this</a>.
# MAGIC 
# MAGIC Consider the model of the data as a constantly appending table. Sorting is one of a handful of operations that is either too complex or logically not possible to do when working with streaming data.
# MAGIC 
# MAGIC A full discussion of these exceptions is out of scope for this course. Note that advanced streaming methods like windowing and watermarking can be used to add additional functionality to incremental workloads.
# MAGIC 
# MAGIC Uncomment and run the following cell how this failure may appear:

# COMMAND ----------

# %sql
# SELECT * 
# FROM streaming_tmp_vw
# ORDER BY time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Persisting Streaming Results
# MAGIC 
# MAGIC In order to persist incremental results, we need to pass our logic back to the PySpark Structured Streaming DataFrames API.
# MAGIC 
# MAGIC Above, we created a temp view from a PySpark streaming DataFrame. If we create another temp view from the results of a query against a streaming temp view, we'll again have a streaming temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts_tmp_vw AS (
# MAGIC   SELECT device_id, COUNT(device_id) AS total_recordings
# MAGIC   FROM streaming_tmp_vw
# MAGIC   GROUP BY device_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC To persist the results of a streaming query, we must write them out to durable storage. The **`DataFrame.writeStream`** method returns a **`DataStreamWriter`** used to configure the output.
# MAGIC 
# MAGIC When writing to Delta Lake tables, we typically will only need to worry about 3 settings, discussed here.
# MAGIC 
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to cloud storage.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams. A checkpoint is required for every streaming write to ensure processing guarantees.
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes" target="_blank">More details here</a>.
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- | --- |
# MAGIC | **Append** | **`.outputMode("append")`**     | **This is the default.** Only newly appended rows are incrementally appended to the target table with each batch |
# MAGIC | **Complete** | **`.outputMode("complete")`** | The Results Table is recalculated each time a write is triggered; the target table is overwritten with each batch |
# MAGIC 
# MAGIC 
# MAGIC ### Trigger Intervals
# MAGIC 
# MAGIC When defining a streaming write, the **`trigger`** method specifies when the system should process the next set of data..
# MAGIC 
# MAGIC 
# MAGIC | Trigger Type                           | Example | Behavior |
# MAGIC |----------------------------------------|----------|----------|
# MAGIC | Unspecified                 |  | **This is the default.** This is equivalent to using **`processingTime="500ms"`** |
# MAGIC | Fixed interval micro-batches      | **`.trigger(processingTime="2 minutes")`** | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | Triggered micro-batch               | **`.trigger(once=True)`** | The query will execute a single micro-batch to process all the available data and then stop on its own |
# MAGIC | Triggered micro-batches       | **`.trigger(availableNow=True)`** | The query will execute multiple micro-batches to process all the available data and then stop on its own |
# MAGIC 
# MAGIC Triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger.
# MAGIC 
# MAGIC **NOTE:** **`Trigger.AvailableNow`**</a> is a new trigger type that is available in DBR 10.1 for Scala only and available in DBR 10.2 and above for Python and Scala.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Pulling It All Together
# MAGIC 
# MAGIC The code below demonstrates using **`spark.table()`** to load data from a streaming temp view back to a DataFrame. Note that Spark will always load streaming views as a streaming DataFrame and static views as static DataFrames (meaning that incremental processing must be defined with read logic to support incremental writing).
# MAGIC 
# MAGIC In this first query, we'll demonstrate using **`trigger(availableNow=True)`** to perform incremental batch processing.

# COMMAND ----------

(spark.table("device_counts_tmp_vw")                               
    .writeStream                                                
    .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("device_counts")
    .awaitTermination() # This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Below, we change our trigger method to change this query from a triggered incremental batch to an always-on query triggered every 4 seconds.
# MAGIC 
# MAGIC **NOTE**: As we start this query, no new records exist in our source table. We'll add new data shortly.

# COMMAND ----------

query = (spark.table("device_counts_tmp_vw")                               
              .writeStream                                                
              .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
              .outputMode("complete")
              .trigger(processingTime='4 seconds')
              .table("device_counts"))

# Like before, wait until our stream has processed some data
DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Querying the Output
# MAGIC Now let's query the output we've written from SQL. Because the result is a table, we only need to deserialize the data to return the results.
# MAGIC 
# MAGIC Because we are now querying a table (not a streaming DataFrame), the following will **not** be a streaming query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC As in our previous lesson, we have configured a helper function to process new records into our source table.
# MAGIC 
# MAGIC Run the cell below to land another batch of data.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Query the target table again to see the updated counts for each **`device_id`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

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
