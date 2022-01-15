# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "{}")""".format(eventsPath))
spark.sql("""CREATE TABLE IF NOT EXISTS sales USING parquet OPTIONS (path "{}")""".format(salesPath))
spark.sql("""CREATE TABLE IF NOT EXISTS users USING parquet OPTIONS (path "{}")""".format(usersPath))
spark.sql("""CREATE TABLE IF NOT EXISTS products USING parquet OPTIONS (path "{}")""".format(productsPath))

displayHTML("")

