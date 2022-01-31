# Databricks notebook source
import pyspark.sql.functions as F
import re

course_name = "eltsql"
source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v02/small-datasets"

username = spark.sql("SELECT current_user()").first()[0]
userhome = f"dbfs:/user/{username}/dbacademy/{course_name}"
database = f"{re.sub('[^a-zA-Z0-9]', '_', username)}_dbacademy_{course_name}"
database_location = f"{userhome}/db"

basepath = f"{userhome}/source_datasets"
source = f"{basepath}/raw"
source_tables = f"{basepath}/delta"
source_database = f"{database}_solution"

print(f"username: {username}")
print(f"database: {database}")
print(f"userhome: {userhome}")

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) >= 0
    except Exception:
        return False

def setup_source(mode):
    if mode == "reset":
        print("Resetting database...")
        dbutils.fs.rm(userhome, True)
        spark.sql(f"DROP DATABASE IF EXISTS {source_database} CASCADE")        
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
        
    if not path_exists(source): 
        print("Installing datasets...")
        dbutils.fs.cp(source_uri, basepath, True)
        
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {source_database} LOCATION '{source_tables}'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{database_location}'")
    
    spark.sql(f"USE {database}")
    spark.sql(f"SET c.source = {source}")
    spark.sql(f"SET c.userhome = {userhome}")
    spark.sql(f"SET c.username = {username}")    
    spark.sql(f"SET c.database = {database}")
    spark.sql(f"SET c.source_tables = {source_tables}")    

dbutils.widgets.text("mode", "default")
mode = dbutils.widgets.get("mode")    
    
setup_source(mode)


