# Databricks notebook source
# MAGIC %md
# MAGIC # Population Heatmap by State
# MAGIC Using *uszips.csv* as a data source, aggregate the populations by state.
# MAGIC Note: data file provided courtesy of SimpleMaps (https://simplemaps.com/data/us-zips)

# COMMAND ----------

# MAGIC %md
# MAGIC Source a Notebook to configure the table name. If `my_name` resides in a different relative path, then adjust the code in **Cmd 3** accordingly.

# COMMAND ----------

# MAGIC %run ./my_name

# COMMAND ----------

# MAGIC %md
# MAGIC Query the table that was named in the `my_name` Notebook. Aggregate the population, grouping by state. For maximum effectiveness, select the **Map** plot to visualize the output.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `state_id` AS `state`,SUM(`population`) AS `population`
# MAGIC FROM ${conf.name}
# MAGIC WHERE `state_id` NOT IN ('AS','GU','MP','PR','VI')
# MAGIC GROUP BY `state`
