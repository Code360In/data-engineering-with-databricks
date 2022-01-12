# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Structured Streaming Concepts
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the programming model used by Spark Structured Streaming
# MAGIC * Configure required options to perform a streaming read on a source
# MAGIC * Describe the requirements for end-to-end fault tolerance
# MAGIC * Configure required options to perform a streaming write to a sink
# MAGIC * Interact with streaming queries and stop active streams
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC The source contains smartphone accelerometer samples from devices and users with the following columns:
# MAGIC 
# MAGIC | Field          | Description |
# MAGIC | ------------- | ----------- |
# MAGIC | Arrival_Time | time data was received |
# MAGIC | Creation_Time | event time |
# MAGIC | Device | type of Model |
# MAGIC | Index | unique identifier of event |
# MAGIC | Model | i.e Nexus4  |
# MAGIC | User | unique user identifier |
# MAGIC | geolocation | city & country |
# MAGIC | gt | transportation mode |
# MAGIC | id | unused null field |
# MAGIC | x | acceleration in x-dir |
# MAGIC | y | acceleration in y-dir |
# MAGIC | z | acceleration in z-dir |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ../Includes/classic-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Micro-Batches as a Table
# MAGIC 
# MAGIC For more information, see the analogous section in the [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts) (from which several images have been borrowed).
# MAGIC 
# MAGIC Spark Structured Streaming approaches streaming data by modeling it as a series of continuous appends to an unbounded table. While similar to defining **micro-batch** logic, this model allows incremental queries to be defined against streaming sources as if they were static input (though the fact that the input is an unbounded tables does impose some constraints).
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png"/>
# MAGIC 
# MAGIC ### Basic Concepts
# MAGIC 
# MAGIC - The developer defines an **input table** by configuring a streaming read against a **source**. The syntax for doing this is similar to working with static data.
# MAGIC - A **query** is defined against the input table. Both the DataFrames API and Spark SQL can be used to easily define transformations and actions against the input table.
# MAGIC - This logical query on the input table generates the **results table**. The results table contains the incremental state information of the stream.
# MAGIC - The **output** of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Generally, a sink will be a durable system such as files or a pub/sub messaging bus.
# MAGIC - New rows are appended to the input table for each **trigger interval**. These new rows are essentially analogous to micro-batch transactions and will be automatically propagated through the results table to the sink.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The `spark.readStream()` method returns a `DataStreamReader` used to configure and query the stream.
# MAGIC 
# MAGIC Configuring a streaming read on a source requires:
# MAGIC * The schema of the data
# MAGIC   * **NOTE**: Some streaming sources allow for schema inference
# MAGIC * The `format` of the source <a href="https://docs.databricks.com/spark/latest/structured-streaming/data-sources.html" target="_blank">file format or named connector</a> 
# MAGIC   * **NOTE**: `delta` is the default format for all reads and writes in Databricks
# MAGIC * Additional source-specific configuration options. For example:
# MAGIC   * [`cloudFiles`](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-s3.html)
# MAGIC   * <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">Kafka</a>
# MAGIC * The name of the source table or the location of the files in object storage
# MAGIC   
# MAGIC Below, we define a streaming read against a source (represented by `dataSource`) consisting of files from cloud storage.
# MAGIC 
# MAGIC **NOTE**: We can think of this `DataStreamReader` as an incremental temp view defined against an ever-appending source table. Just as with a temp view, we only store the query plan when we set up an incremental read. It's not until we query results that we'll see compute happen.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Schema
# MAGIC 
# MAGIC Working with `cloudFiles` allows Databricks to automatically infer the schema from most file sources. 
# MAGIC 
# MAGIC Once data is loaded into a Delta Lake table, all schema for downstream incremental reads will be grabbed automatically from the table metadata.
# MAGIC 
# MAGIC Here, we'll provide an explicit schema for our data.

# COMMAND ----------

schema = """Arrival_Time BIGINT, 
    Creation_Time BIGINT, 
    Device STRING, 
    Index BIGINT, 
    Model STRING, 
    User STRING, 
    geolocation STRUCT<
        city: STRING, 
        country: STRING>, 
    gt STRING, 
    Id BIGINT, 
    x DOUBLE, 
    y DOUBLE, 
    z DOUBLE"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Streaming Temp View
# MAGIC 
# MAGIC Below we pull all of the above concepts together to define a streaming read.
# MAGIC 
# MAGIC If we were continuing to build out our query with PySpark, we would capture this as a DataFrame. Instead, we use `createOrReplaceTempView` to create an entity that can be queried locally with SQL.

# COMMAND ----------

(spark
    .readStream
    .schema(schema)
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(dataSource)
    .createOrReplaceTempView("streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparing to Static Reads
# MAGIC 
# MAGIC The above logic provides us with more or less the same result as the static query below.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TEMP VIEW static_tmp_vw AS SELECT * FROM json.`{dataSource}`")

# COMMAND ----------

# MAGIC %md
# MAGIC When we query a static read on data, we display the results of the query at a point in time.
# MAGIC 
# MAGIC **NOTE**: The `display(spark.table())` pattern shown in the next cell is the same as executing a `SELECT * FROM` for a table or view. Later, we'll see that this allows us to pass streaming temp views back to the DataFrame API to write out a stream.

# COMMAND ----------

display(spark.table("static_tmp_vw"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC But when we execute a query on a streaming temporary view, we'll continue to update the results of the query as new data arrives in the source.
# MAGIC 
# MAGIC Think of a query executed against a streaming temp view as an **always-on incremental query**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Before continuing, click `Stop Execution` at the top of the notebook, `Cancel` immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Streaming Data
# MAGIC We can execute most transformation against streaming temp views the same way we would with static data. Here, we'll run a simple aggregation to get counts of records for each `device`.
# MAGIC 
# MAGIC Because we are querying a streaming temp view, this becomes a streaming query that executes indefinitely, rather than completing after retrieving a single set of results. For streaming queries like this, Databricks Notebooks include interactive dashboards that allow users to monitor streaming performance. Explore this below.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/adbcore/streaming-dashboard.png)
# MAGIC 
# MAGIC One important note regarding this example: this is merely displaying an aggregation of input as seen by the stream. **None of these records are being persisted anywhere at this point.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device, COUNT(device) AS total_records
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY device

# COMMAND ----------

# MAGIC %md
# MAGIC Before continuing, click `Stop Execution` at the top of the notebook, `Cancel` immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persisting Streaming Results
# MAGIC 
# MAGIC In order to persist incremental results, we need to pass our logic back to the PySpark Structured Streaming DataFrames API.
# MAGIC 
# MAGIC Above, we created a temp view from a PySpark streaming DataFrame. If we create another temp view from the results of a query against a streaming temp view, we'll again have a streaming temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts_tmp_vw AS (
# MAGIC   SELECT device, COUNT(device) AS total_records
# MAGIC   FROM streaming_tmp_vw
# MAGIC   GROUP BY device
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC To persist the results of a streaming query, we must write them out to durable storage. The `DataFrame.writeStream` method returns a `DataStreamWriter` used to configure the output.
# MAGIC 
# MAGIC There are a number of required parameters to configure a streaming write:
# MAGIC * The `format` of the **output sink**; see <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks" target="_blank">documentation</a>
# MAGIC * The location of the **checkpoint directory**
# MAGIC * The **output mode**
# MAGIC * Configurations specific to the output sink, such as:
# MAGIC   * <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">Kafka</a>
# MAGIC   * A <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=foreach#pyspark.sql.streaming.DataStreamWriter.foreach"target="_blank">custom sink</a> via `writeStream.foreach(...)`
# MAGIC 
# MAGIC Once the configuration is completed, we trigger the job with a call to `.table()`. If we didn't want to create a table and instead wanted to write directly to storage, we would use `.start()` instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to cloud storage.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. [More details here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes).
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- | --- |
# MAGIC | **Append** | `.outputMode("append")`     | Only the new rows appended to the Result Table since the last trigger are written to the sink. This is the default. |
# MAGIC | **Complete** | `.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
# MAGIC | **Update** | `.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink.|
# MAGIC 
# MAGIC **NOTE**: Not all sinks will support `update` mode.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Defining the Trigger Interval
# MAGIC 
# MAGIC When defining a streaming write, the `trigger` method specifies when the system should process the next set of data..
# MAGIC 
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | The query will be executed as soon as the system has completed processing the previous query (this is the default) |
# MAGIC | Fixed interval micro-batches           | `.trigger(processingTime="2 minutes")` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(once=True)` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
# MAGIC | Continuous w/fixed checkpoint interval | `.trigger(continuous="1 second")` | The query will be executed in a low-latency, <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing" target = "_blank">continuous processing mode</a>. _EXPERIMENTAL_ |
# MAGIC 
# MAGIC Note that triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger; some sources allow configuration to limit the size of each micro-batch.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_best_24.png"/> Read <a href="https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html" target="_blank">this blog post</a> to learn more about using one-time triggers to simplify CDC with a hybrid streaming/batch design.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pulling It All Together
# MAGIC 
# MAGIC The code below demonstrates using `spark.table()` to load data from a streaming temp view back to a DataFrame. Note that Spark will always load streaming views as a streaming DataFrame and static views as static DataFrames (meaning that incremental processing must be defined with read logic to support incremental writing).

# COMMAND ----------

streamingQuery = (spark.table("device_counts_tmp_vw")                               
  .writeStream                                                
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  .trigger(processingTime='10 seconds')
  .table("device_counts")
)

# COMMAND ----------

# MAGIC %md
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
# MAGIC ## Debugging with the Memory Sink
# MAGIC 
# MAGIC The **memory** sink can be a useful tool for debugging. It provides a quick and easy sink requiring no setup. The output is stored as an in-memory table, with a name defined using `queryName`.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> This should be used only for debugging purposes with low data volumes, since the entire output is collected and stored in the driver’s memory.

# COMMAND ----------

streamingQueryMem = (spark.table("streaming_tmp_vw")                                
  .writeStream                                                
  .format("memory")                 # memory = store in-memory table (for testing only)                                          
  .queryName("streaming_query_mem") # name of the in-memory table
  .outputMode("append")
  .start()                                       
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's examine the contents of the in-memory table with the same query used previously. Like the previous query we ran against the Delta output, this will **not** be a streaming query. In this case, we are simply querying the in-memory table established by the memory sink in the previous cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device, COUNT(device) AS total_records
# MAGIC FROM streaming_query_mem
# MAGIC GROUP BY device

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interacting with Streaming Queries
# MAGIC 
# MAGIC 
# MAGIC the logic defined above, data is read from JSON files and then saved out in the Delta Lake format. Note that because Delta creates a new version for each transaction, when working with streaming data this will mean that the Delta table creates a new version for each trigger interval in which new data is processed. [More info on streaming with Delta](https://docs.databricks.com/delta/delta-streaming.html#table-streaming-reads-and-writes).

# COMMAND ----------

# MAGIC %md
# MAGIC The `recentProgress` attribute allows access to metadata about recently processed micro-batches. Let's dump the contents for the streaming query created earlier.

# COMMAND ----------

streamingQuery.recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC In addition to referencing `StreamingQuery` objects returned by `writeStream`, as we did above, we can iterate on the `streams.active` attribute in `SparkSession` to identify all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
    print(s.id)                        # Print the stream's id

# COMMAND ----------

# MAGIC %md
# MAGIC Let's iterate on all active streams and stop them. This is an important thing to do here, for if you don't then your cluster will run indefinitely!
# MAGIC 
# MAGIC After running the following cell, feel free to examine the cells earlier that initiated streaming queries; notice they have both been canceled.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Ingestion with Auto Loader
# MAGIC 
# MAGIC Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data is key to achieving scalability.
# MAGIC 
# MAGIC Ingesting into a Delta Lake table from a data lake is a common use case that has traditionally been challenging to properly set up, typically relying on the integration of always-on services like Kafka to track the files that have been ingested, and to monitor cloud storage for new file arrivals. Databricks Auto Loader abstracts all this and provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage, in the form of a structured streaming source.
# MAGIC 
# MAGIC Given an input directory path on the cloud file storage, the `cloudFiles` source automatically processes new files as they arrive, with the option of also processing existing files in that directory. For full details, refer to the <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentation</a>.
# MAGIC 
# MAGIC **Due to the benefits and scalability that Auto Loader delivers, Databricks recommends its use as general best practice when ingesting data from cloud storage.**

# COMMAND ----------

# MAGIC %md
# MAGIC Reset the output directory in preparation to stream data using Auto Loader.

# COMMAND ----------

# MAGIC %run ../Includes/classic-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data with Auto Loader
# MAGIC 
# MAGIC An example invocation of Auto Loader is provided below. Comparing against the standard streaming read from earlier, notice the following differences:
# MAGIC 
# MAGIC * Specify a `format` of `cloudFiles`
# MAGIC * Specify the underlying format of the data using the `cloudFiles.format` option
# MAGIC * The `dataLandingLocation` source below represents a cloud storage location from where data is being ingested

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Schema Inference and Evolution
# MAGIC 
# MAGIC As mentioned earlier, every streaming DataFrame must have a schema. But Auto Loader can be configured to take a more active role in inferring and maintaining the schema of the data as it evolves.
# MAGIC 
# MAGIC By omitting a schema specification, Auto Loader will detect the schema based on the data seen on the input. Specifying the `cloudFiles.schemaLocation` option allows Auto Loader to track the schema, thereby improving performances and ensuring stability of the schema across stream restart. A common pattern is to use `checkpointLocation` for this purpose.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> There must be data present for schema inference to work; otherwise you must specify a schema.
# MAGIC 
# MAGIC **Schema evolution** allows changes to a schema in response to data that changes over time. This can be an important feature in some use cases.

# COMMAND ----------

incrementalStreamingDF = (spark
  .readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpointPath)
  .load(dataLandingLocation)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the output also takes a similar form as the previous streaming case. Note the following differences:
# MAGIC * Specify `mergeSchema` option to activate schema evolution. If any changes to the schema occur over time, the schema is adapted rather than rejecting the write. This can be useful in some use cases.
# MAGIC * Omitting the trigger to allow the query to continue running, ingesting new data as it arrives. If you wish to schedule your ETL process to run in batch mode, consider using a one-time trigger instead.

# COMMAND ----------

(incrementalStreamingDF
 .writeStream
 .format("delta")                                          
 .option("checkpointLocation", checkpointPath)
 .option("mergeSchema", "true")
 .table(outputTable)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying the Output
# MAGIC By now the following query against the output table will likely seem familiar. Run it a few times, and it will become apparent that nothing changes, as no data is arriving in our simulated cloud storage.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Device,COUNT(Device) AS Count
# MAGIC FROM ${c.outputTable}
# MAGIC GROUP BY Device

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land New Data
# MAGIC Run the following cell to simulate the arrival of new data in our cloud storage. Each time you execute the cell below, a new file will be written to our source directory. Following this cell, observe the stream monitor above, and notice the impact on the results when re-running the query.

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up
# MAGIC Stop active streams and remove created resources before continuing.

# COMMAND ----------

# MAGIC %run ../Includes/classic-setup $mode="clean"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
# MAGIC 
# MAGIC We used `readStream` to stream input from a variety of sources, including Databricks Auto Loader. Auto Loader augments Structured Streaming functionality by providing an easy-to-use interface of performing incremental ETL from cloud storage. 
# MAGIC 
# MAGIC We also explored various options for consuming, writing and querying the streamed input data.
# MAGIC 
# MAGIC Finally, we explored the array of active streams maintained in the `SparkSession` object.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
# MAGIC 
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a><br>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#id2" target="_blank">Failed Streaming Query Recovery</a> Best Practices for Recovery.
# MAGIC * <a href="https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html" target="_blank">Continuous Processing Mode</a> Lowest possible latency stream processing.  Currently Experimental.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
