# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="6.3L"

# COMMAND ----------

DA.cleanup()
DA.init()
DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"    
DA.conclude_setup()

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

