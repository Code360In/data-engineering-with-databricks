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

outputPath = userhome + "/streaming-concepts"
checkpointPath = outputPath + "/checkpoint"

# original dataset
dataSource = "/mnt/training/definitive-guide/data/activity-json/streaming"

# data landing location; files will be copies from original dataset one at a time for incremental ingestion use case
dataLandingLocation = outputPath + "/landing-zone"

outputTable = "bronze_table"

spark.conf.set('c.outputTable', outputTable)

# COMMAND ----------

class FileArrival:
    def __init__(self, dataSource, landingZone):
        self.sourceFiles = dbutils.fs.ls(dataSource)
        dbutils.fs.mkdirs(landingZone)
        self.landingZone = landingZone
        self.fileID = 0
    
    def newData(self, numFiles=1):
        for i in range(numFiles):
            dbutils.fs.cp(self.sourceFiles[self.fileID].path, self.landingZone)
            self.fileID+=1

# COMMAND ----------

File = FileArrival(dataSource, dataLandingLocation)
File.newData()

