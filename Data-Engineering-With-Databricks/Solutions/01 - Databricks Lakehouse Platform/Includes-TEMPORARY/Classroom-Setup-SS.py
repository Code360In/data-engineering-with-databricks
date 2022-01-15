# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

couponsCheckpointPath = workingDir + "/coupon-sales/checkpoint"
couponsOutputPath = workingDir + "/coupon-sales/output"

deltaEventsPath = workingDir + "/delta/events"
deltaSalesPath = workingDir + "/delta/sales"
deltaUsersPath = workingDir + "/delta/users"

displayHTML("")

