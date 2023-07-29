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
# MAGIC # Incremental Multi-Hop in the Lakehouse
# MAGIC 
# MAGIC Now that we have a better understanding of how to work with incremental data processing by combining Structured Streaming APIs and Spark SQL, we can explore the tight integration between Structured Streaming and Delta Lake.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe Bronze, Silver, and Gold tables
# MAGIC * Create a Delta Lake multi-hop pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Incremental Updates in the Lakehouse
# MAGIC 
# MAGIC Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage of the pipeline represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, to name a few examples).
# MAGIC 
# MAGIC - **Silver** tables provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
# MAGIC 
# MAGIC - **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. 
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data.
# MAGIC 
# MAGIC Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
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
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Data Simulator
# MAGIC Databricks Auto Loader can automatically process files as they land in your cloud object stores. 
# MAGIC 
# MAGIC To simulate this process, you will be asked to run the following operation several times throughout the course.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Bronze Table: Ingesting Raw JSON Recordings
# MAGIC 
# MAGIC Below, we configure a read on a raw JSON source using Auto Loader with schema inference.
# MAGIC 
# MAGIC Note that while you need to use the Spark DataFrame API to set up an incremental read, once configured you can immediately register a temp view to leverage Spark SQL for streaming transformations on your data.
# MAGIC 
# MAGIC **NOTE**: For a JSON data source, Auto Loader will default to inferring each column as a string. Here, we demonstrate specifying the data type for the **`time`** column using the **`cloudFiles.schemaHints`** option. Note that specifying improper types for a field will result in null values.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Here, we'll enrich our raw data with additional metadata describing the source file and the time it was ingested. This additional metadata can be ignored during downstream processing while providing useful information for troubleshooting errors if corrupt data is encountered.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC The code below passes our enriched raw data back to PySpark API to process an incremental write to a Delta Lake table.

# COMMAND ----------

(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Trigger another file arrival with the following cell and you'll see the changes immediately detected by the streaming query you've written.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Load Static Lookup Table
# MAGIC The ACID guarantees that Delta Lake brings to your data are managed at the table level, ensuring that only fully successfully commits are reflected in your tables. If you choose to merge these data with other data sources, be aware of how those sources version data and what sort of consistency guarantees they have.
# MAGIC 
# MAGIC In this simplified demo, we are loading a static CSV file to add patient data to our recordings. In production, we could use Databricks' <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> feature to keep an up-to-date view of these data in our Delta Lake.

# COMMAND ----------

(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.data_source}/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Silver Table: Enriched Recording Data
# MAGIC As a second hop in our silver level, we will do the follow enrichments and checks:
# MAGIC - Our recordings data will be joined with the PII to add patient names
# MAGIC - The time for our recordings will be parsed to the format **`'yyyy-MM-dd HH:mm:ss'`** to be human-readable
# MAGIC - We will exclude heart rates that are <= 0, as we know that these either represent the absence of the patient or an error in transmission

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Trigger another new file and wait for it propagate through both previous queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Gold Table: Daily Averages
# MAGIC 
# MAGIC Here we read a stream of data from **`recordings_enriched`** and write another stream to create an aggregate gold table of daily averages for each patient.

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note that we're using **`.trigger(availableNow=True)`** below. This provides us the ability to continue to use the strengths of structured streaming while trigger this job one-time to process all available data in micro-batches. To recap, these strengths include:
# MAGIC - exactly once end-to-end fault tolerant processing
# MAGIC - automatic detection of changes in upstream data sources
# MAGIC 
# MAGIC If we know the approximate rate at which our data grows, we can appropriately size the cluster we schedule for this job to ensure fast, cost-effective processing. The customer will be able to evaluate how much updating this final aggregate view of their data costs and make informed decisions about how frequently this operation needs to be run.
# MAGIC 
# MAGIC Downstream processes subscribing to this table do not need to re-run any expensive aggregations. Rather, files just need to be de-serialized and then queries based on included fields can quickly be pushed down against this already-aggregated source.

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC #### Important Considerations for complete Output with Delta
# MAGIC 
# MAGIC When using **`complete`** output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
# MAGIC 
# MAGIC **NOTE**: Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignoring Updates and Deletes</a>.
# MAGIC 
# MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Note the above table includes all days for all users. If the predicates for our ad hoc queries match the data encoded here, we can push down our predicates to files at the source and very quickly generate more limited aggregate views.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Process Remaining Records
# MAGIC The following cell will land additional files for the rest of 2020 in your source directory. You'll be able to see these process through the first 3 tables in your Delta Lake, but will need to re-run your final query to update your **`daily_patient_avg`** table, since this query uses the trigger available now syntax.

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Finally, make sure all streams are stopped.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC Delta Lake and Structured Streaming combine to provide near real-time analytic access to data in the lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Table Streaming Reads and Writes</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Create a Kafka Source Stream</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
