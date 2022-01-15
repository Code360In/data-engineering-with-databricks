# Databricks notebook source
# Does any work to reset the environment prior to testing.
import re, time

try:
    dbutils.fs.unmount("/mnt/training")
    time.sleep(15) # for some reason things are moving too fast
except:
    print("/mnt/training isn't mounted")

try:
    dbutils.fs.ls("/mnt/training")
    raise Exception("Mount shouldn't exist")
except:
    # This is good, mount has been removed
    print("/mnt/training confirmed to not exist")

# Drop any lingering databases
username = spark.sql("SELECT current_user()").first()[0].lower()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
database_prefix = f"dbacademy_{clean_username}_aspwd"

for database in spark.catalog.listDatabases():
    if database.name.startswith(database_prefix):
        print(f"Dropping {database.name}")
        spark.sql(f"DROP DATABASE IF EXISTS {database.name} CASCADE")
        
working_dir = f"dbfs:/user/{username}/dbacademy/aspwd"
if dbutils.fs.rm(working_dir, True):
    print(f"Deleted working dir: {working_dir}")


# COMMAND ----------

# MAGIC %run ./Classroom-Setup
