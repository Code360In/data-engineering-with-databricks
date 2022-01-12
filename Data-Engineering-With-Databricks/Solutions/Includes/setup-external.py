# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

if mode != "clean":      
    dbutils.fs.rm(f"{Paths.source}/sales/sales.csv", True)
    dbutils.fs.cp(f"{Paths.source_uri}/sales/sales.csv", f"{Paths.source}/sales/sales.csv", True)   

    (spark
        .read
        .format("parquet")
        .load(f"{Paths.source}/users/users.parquet")
        .repartition(1)
        .write
        .format("org.apache.spark.sql.jdbc")
        .option("url", f"jdbc:sqlite:/{username}_ecommerce.db")
        .option("dbtable", "users")
        .mode("overwrite")
        .save())

