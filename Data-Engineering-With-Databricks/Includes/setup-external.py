# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

dbutils.fs.rm(f"{source}/sales-csv", True)
dbutils.fs.cp(f"{source_uri}/raw/sales-csv", f"{source}/sales-csv", True)   

(spark
    .read
    .format("parquet")
    .load(f"{source}/users-historical/")
    .repartition(1)
    .write
    .format("org.apache.spark.sql.jdbc")
    .option("url", f"jdbc:sqlite:/{username}_ecommerce.db")
    .option("dbtable", "users")
    .mode("overwrite")
    .save())


