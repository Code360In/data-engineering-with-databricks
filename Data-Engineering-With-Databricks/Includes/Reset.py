# Databricks notebook source
import pyspark.sql.functions as F
import re

course_name = "eltsql"

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
database = f"""{clean_username}_dbacademy_{course_name}"""
userhome = f"dbfs:/user/{username}/dbacademy/{course_name}"

print(f"username:       {username}")
print(f"clean_username: {clean_username}")
print(f"database:       {database}")
print(f"userhome:       {userhome}")

dbutils.fs.rm(userhome, True)

print(f"Dropping the database {database}")
spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

