# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Windows and Watermarks
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Explain why some methods will not work on streaming data
# MAGIC * Use windows to aggregate over chunks of data rather than all data
# MAGIC * Compare tumbling windows and sliding windows
# MAGIC * Apply watermarking to manage state

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
# MAGIC ## Configure Streaming Read
# MAGIC 
# MAGIC This lesson uses the same data as the previous notebook, again loaded with AutoLoader.
# MAGIC 
# MAGIC The code below registers a streaming DataFrame (which we'll use again in a moment) and a streaming temp view.
# MAGIC 
# MAGIC Note the use of the `selectExpr` method, which allows multiple SQL operations to be configured on a per column basis in PySpark DataFrames. Here, we're simplifying the data to be dealt with by selecting only two columns:
# MAGIC * `Creation_Time`, originally encoded in nanoseconds, is converted to unixtime and renamed to `creation_time`
# MAGIC * `gt` is renamed to `action`

# COMMAND ----------

from pyspark.sql.functions import col

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

streamingDF = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(dataLandingLocation)
    .selectExpr("cast(Creation_Time/1E9 AS timestamp) AS creation_time", "gt AS action")       
)

streamingDF.createOrReplaceTempView("streaming_tmp_vw") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsupported Operations
# MAGIC 
# MAGIC Most operations on a streaming DataFrame are identical to a static DataFrame. There are <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">some exceptions to this</a>.
# MAGIC 
# MAGIC Consider the model of the data as a constantly appending table. Sorting is one of a handful of operations that is either too complex or logically not possible to do when working with streaming data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw
# MAGIC ORDER BY creation_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Streaming Aggregations
# MAGIC 
# MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
# MAGIC 
# MAGIC Some examples include
# MAGIC * Aggregating errors in data from IoT devices by type
# MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country
# MAGIC * Performing behavior analysis on instant messages via hash tags
# MAGIC 
# MAGIC While these streaming aggregates may need to reference historic trends, analytics will generally be calculated over discrete units of time. Spark Structured Streaming supports time-based **windows** on streaming DataFrames to make these calculations easy.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What is Time?
# MAGIC 
# MAGIC Multiple times may be associated with each streaming event. Consider the discrete differences between the time at which the event data was:
# MAGIC - Generated
# MAGIC - Written to the streaming source
# MAGIC - Processed into Spark
# MAGIC 
# MAGIC Each of these times will be recorded from the system clock of the machine running the process, with discrepancies and latencies being introduced due to many different causes. 
# MAGIC 
# MAGIC Generally speaking, most analytics will be interested in the time the data was generated. As such, this lesson will focus on timestamps recorded at the time of data generation, which we will refer to as the **event time**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Windowing
# MAGIC 
# MAGIC Defining windows on a time series field imposes a time range constraint on an otherwise unbounded input. This allows users to utilize this field for aggregations in the same way they would use distinct values when calling `GROUP BY`. Spark maintains a state table with aggregates for each user-defined bucket of time.
# MAGIC 
# MAGIC Spark supports three types of windows:
# MAGIC 
# MAGIC * **Tumbling windows**: fixed-size windows, regularly recurring windows that do not overlap. Each event will be aggregated into only one window. 
# MAGIC * **Sliding windows**: fixed-size windows, regularly recurring windows that overlap. Each event may be aggregated into multiple windows. 
# MAGIC * **Session windows**: dynamic windows whose start time and duration depends on the inputs. An event will strigger the start of a window that will, in general, continue until a predetermined duration after the last event received.
# MAGIC 
# MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-time-window-types.jpg">

# COMMAND ----------

# MAGIC %md
# MAGIC The following diagram illustrates in greater detail the concept of sliding windows and how events received at various times will be aggregated into the various windows (assuming that the slide duration is less than the window duration, which leads to overlapping windows):
# MAGIC 
# MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-window.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Consuming with a Windowed Aggregation
# MAGIC 
# MAGIC Let's consume the stream from SQL in a windowed aggregation using the SQL `window` function, which accepts a timestamp column and a window duration to define the tumbling windows. An optional third argument specifies a slide duration that allows the definition of a sliding window.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   window.start AS start,
# MAGIC   action,
# MAGIC   count(action) AS count
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY
# MAGIC   window(creation_time, '1 hour'),
# MAGIC   action
# MAGIC ORDER BY
# MAGIC   start,
# MAGIC   action

# COMMAND ----------

# MAGIC %md
# MAGIC Once a batch of data has loaded, render the results as a bar graph with the following settings:
# MAGIC 
# MAGIC * **Keys** is set to `start`
# MAGIC * **Series groupings** is set to `action`
# MAGIC * **Values** is set to `count`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Land New Data
# MAGIC Recall that our stream has been set up for incremental ingestion. Invoke the following cell a few times to simulate the arrival of new data. Note the impact on the results reported above.

# COMMAND ----------

File.newData()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Considerations
# MAGIC Because aggregation is a <a href="https://databricks.com/glossary/what-are-transformations" target="_blank">wide transformation</a>, it will trigger a shuffle. Configuring the number of partitions can reduce the number of tasks and properly balance the workload for the cluster.
# MAGIC 
# MAGIC In most cases, a 1-to-1 mapping of partitions to cores is ideal for streaming applications. The code below sets the number of partitions to 4, which maps perfectly to a cluster with 4 cores.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Watermarking
# MAGIC 
# MAGIC 
# MAGIC When aggregating with an unbounded input, Spark's fault-tolerant state management naturally incurs some processing overhead. To keep these overheads bounded within acceptable limits, the size of the state data should not grow indefinitely. However, with sliding windows, the number of windows/groups will grow indefinitely, and so can the size of state (proportional to the number of groups). To bound the state size, we have to be able to drop old aggregates that are not going to be updated anymore. We achieve this using **watermarking**.
# MAGIC 
# MAGIC Watermarking allows users to define a cutoff threshold for how much state should be maintained. This cutoff is calculated against the most recently seen event time. Data arriving after this threshold will be discarded.
# MAGIC 
# MAGIC The <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withWatermark.html" target="_blank">`withWatermark`</a> method allows users to easily define this cutoff threshold.
# MAGIC 
# MAGIC Note that there is no built-in support for watermarking in Spark SQL, but we can define this in PySpark before creating a temp view, as shown below.

# COMMAND ----------

(streamingDF
    .withWatermark("creation_time", "2 hours")       # Specify a 2-hour watermark
    .createOrReplaceTempView("watermarked_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC By directing our windowed aggregation at this new temp view, we can easily achieve the same outcome while managing state information.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   window.start AS start,
# MAGIC   action,
# MAGIC   count(action) AS count
# MAGIC FROM watermarked_tmp_vw
# MAGIC GROUP BY
# MAGIC   window(creation_time, '1 hour'),
# MAGIC   action
# MAGIC ORDER BY
# MAGIC   start,
# MAGIC   action

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Example Details
# MAGIC 
# MAGIC The threshold is always calculated against the max event time seen.
# MAGIC 
# MAGIC In the example above,
# MAGIC * The in-memory state is limited to two hours of historic data.
# MAGIC * Data arriving more than 2 hours late should be dropped.
# MAGIC * Data received within 2 hours of being generated will never be dropped.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This guarantee is strict in only one direction. Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated. The more delayed the data is, the less likely the engine is going to process it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Results
# MAGIC 
# MAGIC Previously we used `spark.table()` to pass SQL logic stored in temp views back to a DataFrame to write out streaming results.
# MAGIC 
# MAGIC Below, we instead use `spark.sql()` and pass the entire SQL query.

# COMMAND ----------

(spark.sql("""
    SELECT
      window.start AS start,
      action,
      count(action) AS count
    FROM watermarked_tmp_vw
    GROUP BY
      window(creation_time, '1 hour'),
      action
    ORDER BY
      start,
      action
""").writeStream
    .option("checkpointLocation", checkpointPath)
    .outputMode("complete")
    .table("action_counts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %run ../Includes/classic-setup $mode="clean"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC * A handful of operations valid for static DataFrames will not work with streaming data
# MAGIC * Windows allow users to define time-based buckets for aggregating streaming data
# MAGIC * Watermarking allows users to manage the amount of state being calculated with each trigger and define how late-arriving data should be handled

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
