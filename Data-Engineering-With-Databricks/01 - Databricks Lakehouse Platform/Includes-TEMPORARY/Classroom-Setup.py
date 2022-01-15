# Databricks notebook source
# MAGIC %run ./Common-Notebooks/Common

# COMMAND ----------

salesPath =    f"{datasetsDir}/sales/sales.parquet"
spark.sql(f"SET c.sales_path = {salesPath}")

usersPath =    f"{datasetsDir}/users/users.parquet"
spark.sql(f"SET c.users_path = {usersPath}")

eventsPath =   f"{datasetsDir}/events/events.parquet"
spark.sql(f"SET c.events_path = {eventsPath}")

productsPath = f"{datasetsDir}/products/products.parquet"
spark.sql(f"SET c.products_path = {productsPath}")

