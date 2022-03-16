# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="3.2"

# COMMAND ----------

# We want the state from the previous lesson
# DA.cleanup() # DO NOT EXECUTE

tags = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(dbutils.entry_point.getDbutils().notebook().getContext().tags())
is_job = "jobId" in tags
if is_job: print("Mocking global temp view")

DA.init(create_db=is_job)

if is_job:
    spark.sql(f"""USE {DA.db_name}""")
    spark.sql("""CREATE TABLE IF NOT EXISTS external_table USING CSV OPTIONS (path '${da.paths.working_dir}/flight_delays', header "true", mode "FAILFAST");""")
    spark.sql("""CREATE OR REPLACE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 AS SELECT * FROM external_table WHERE distance > 1000;""")

print()
DA.conclude_setup()

