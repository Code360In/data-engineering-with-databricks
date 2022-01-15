-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE recordings_bronze
AS SELECT current_timestamp() receipt_time, "recordings" dataset, *
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW pii
AS SELECT *
  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE recordings_parsed
AS SELECT 
  CAST(device_id AS INTEGER) device_id, 
  CAST(mrn AS LONG) mrn, 
  CAST(heartrate AS DOUBLE) heartrate, 
  CAST(FROM_UNIXTIME(time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time 
  FROM STREAM(live.recordings_bronze)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE recordings_enriched
  (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
AS SELECT device_id, a.mrn, name, time, heartrate
  FROM STREAM(live.recordings_parsed) a
  INNER JOIN STREAM(live.pii) b
  ON a.mrn = b.mrn

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE daily_patient_avg
  COMMENT "Daily mean heartrates by patient"
AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
  FROM STREAM(live.recordings_enriched)
  GROUP BY mrn, name, DATE(time)

