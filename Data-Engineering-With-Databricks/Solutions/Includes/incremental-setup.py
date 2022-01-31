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

source_path = userhome + "/source/"
bronze_checkpoint = userhome + "/checkpoints/bronze"
silver_checkpoint = userhome + "/checkpoints/silver"

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .load(data_source)
        .writeStream
        .option("checkpointLocation", checkpoint_directory)
        .option("mergeSchema", "true")
        .trigger(once=True)
        .table(table_name)
        .awaitTermination()
    )

# COMMAND ----------

class FileArrival:
    def __init__(self):
        self.source = "/mnt/training/healthcare/tracker/streaming/"
        self.userdir = source_path
        self.curr_mo = 1
    
    def newData(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
                self.curr_mo += 1
                autoload_to_table(self.userdir, "json", "bronze", bronze_checkpoint)
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
            self.curr_mo += 1
            autoload_to_table(self.userdir, "json", "bronze", bronze_checkpoint)
        

# COMMAND ----------

File = FileArrival()

if mode != "clean":
    File.newData()

