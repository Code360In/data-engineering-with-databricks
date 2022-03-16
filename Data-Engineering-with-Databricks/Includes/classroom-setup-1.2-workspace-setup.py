# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="1.2"

# COMMAND ----------

def create_demo_tmp_vw():
    print("Creating the temp view demo_tmp_vw")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW demo_tmp_vw(name, value) AS VALUES
        ("Yi", 1),
        ("Ali", 2),
        ("Selina", 3)
        """)

# COMMAND ----------

DA.init(create_db=False)
create_demo_tmp_vw()
DA.conclude_setup()

