# Databricks notebook source
for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

import pyspark.sql.functions as F
import re

course_name = "dewd"

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
database = f"dbacademy_{clean_username}_{course_name}"

userhome = f"dbfs:/user/{username}/dbacademy/{course_name}"

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset" or mode == "clean":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

if mode != "clean":
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# COMMAND ----------

dataSource = "/mnt/training/healthcare"

dataLandingLocation    = userhome + "/source"
bronzePath             = userhome + "/bronze"
recordingsParsedPath   = userhome + "/silver/recordings_parsed"
recordingsEnrichedPath = userhome + "/silver/recordings_enriched"
dailyAvgPath           = userhome + "/gold/dailyAvg"

checkpointPath               = userhome + "/checkpoints"
bronzeCheckpoint             = userhome + "/checkpoints/bronze"
recordingsParsedCheckpoint   = userhome + "/checkpoints/recordings_parsed"
recordingsEnrichedCheckpoint = userhome + "/checkpoints/recordings_enriched"
dailyAvgCheckpoint           = userhome + "/checkpoints/dailyAvgPath"

# COMMAND ----------

class FileArrival:
  def __init__(self):
    self.source = dataSource + "/tracker/streaming/"
    self.userdir = dataLandingLocation + "/"
    self.curr_mo = 1
    
  def newData(self, continuous=False):
    if self.curr_mo > 12:
      print("Data source exhausted\n")
    elif continuous == True:
      while self.curr_mo <= 12:
        curr_file = f"{self.curr_mo:02}.json"
        dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
        self.curr_mo += 1
    else:
      curr_file = f"{str(self.curr_mo).zfill(2)}.json"
      dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
      self.curr_mo += 1
      
File = FileArrival()

