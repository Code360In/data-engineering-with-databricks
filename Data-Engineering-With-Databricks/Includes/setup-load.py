# Databricks notebook source
# MAGIC %run ./setup-migrate

# COMMAND ----------

def kafka_to_bronze():

  spark.sql(f"""
  CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition BIGINT, timestamp BIGINT, topic STRING, value BINARY, date DATE)
  USING DELTA
  LOCATION "{Paths.events_raw_table_path}"
  """)
  
  kafka_schema = """
  key BINARY,
  offset BIGINT,
  partition BIGINT,
  timestamp BIGINT,
  topic STRING,
  value BINARY
  """
  
  (spark.read
    .format("json")
    .schema(kafka_schema)
    .load(f"{Paths.source}/events/events-kafka.json")
    .withColumn("date", F.to_date((F.col("timestamp") / 1e3).cast("timestamp")))
    .write
    .format("delta")
    .mode("append")
    .save(Paths.events_raw_table_path))

# COMMAND ----------

def users_update_view():
  (spark
    .read
    .format("parquet")
    .load(f"{Paths.source}/users/users-30m.parquet")
    .withColumn("updated", F.current_timestamp())
    .createOrReplaceTempView("users_update"))

# COMMAND ----------

def events_json_view():
  silverSchema = spark.table("events_clean").schema

  (spark
    .read
    .table("events_raw")
    .withColumn("json", F.from_json(F.col("value").cast("STRING"), silverSchema))
    .createOrReplaceTempView("json_payload"))

# COMMAND ----------

def update_sales():
  spark.sql(f"""
    COPY INTO sales
    FROM "{Paths.source}/sales/sales-30m.parquet"
    FILEFORMAT = PARQUET
  """)

# COMMAND ----------

if mode != "clean":
  kafka_to_bronze()
  users_update_view()
  events_json_view()
  update_sales()


