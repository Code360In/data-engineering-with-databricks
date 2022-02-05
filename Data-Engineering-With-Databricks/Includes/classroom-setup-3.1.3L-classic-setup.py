# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="3.1.3L"

# COMMAND ----------

DA.cleanup()
DA.init()
DA.conclude_setup()

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

