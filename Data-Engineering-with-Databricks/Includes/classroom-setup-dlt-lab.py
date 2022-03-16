# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_lab"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def print_sql(rows, sql):
    displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")
    

# COMMAND ----------

generate_register_dlt_event_metrics_sql_string = ""

def _generate_register_dlt_event_metrics_sql():
    global generate_register_dlt_event_metrics_sql_string
    
    generate_register_dlt_event_metrics_sql_string = f"""
CREATE TABLE IF NOT EXISTS {DA.db_name}.dlt_events
LOCATION '{DA.paths.storage_location}/system/events';

CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_success AS
SELECT * FROM {DA.db_name}.dlt_events
WHERE details:flow_progress:metrics IS NOT NULL;

CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_metrics AS
SELECT timestamp, origin.flow_name, details 
FROM {DA.db_name}.dlt_success
ORDER BY timestamp DESC;""".strip()
    
    print_sql(13, generate_register_dlt_event_metrics_sql_string)
DA.generate_register_dlt_event_metrics_sql = _generate_register_dlt_event_metrics_sql

# COMMAND ----------

def _generate_daily_patient_avg():
    sql = f"SELECT * FROM {DA.db_name}.daily_patient_avg"
    print_sql(3, sql)

DA.generate_daily_patient_avg = _generate_daily_patient_avg

# COMMAND ----------

def _generate_visualization_query():
    sql = f"""
SELECT flow_name, timestamp, int(details:flow_progress:metrics:num_output_rows) num_output_rows
FROM {DA.db_name}.dlt_metrics
ORDER BY timestamp DESC;"""
    
    print_sql(5, sql)

DA.generate_visualization_query = _generate_visualization_query

# COMMAND ----------

class DataFactory:
    def __init__(self):
        self.source = f"{DA.paths.data_source}/tracker/streaming/"
        self.userdir = DA.paths.data_landing_location
        try:
            self.curr_mo = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.userdir)]))
        except:
            self.curr_mo = 1
    
    def load(self, continuous=False):
        if self.curr_mo > 12:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_mo <= 12:
                curr_file = f"{self.curr_mo:02}.json"
                target_dir = f"{self.userdir}/{curr_file}"
                print(f"Loading the file {curr_file} to the {target_dir}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
                self.curr_mo += 1
        else:
            curr_file = f"{str(self.curr_mo).zfill(2)}.json"
            target_dir = f"{self.userdir}/{curr_file}"
            print(f"Loading the file {curr_file} to the {target_dir}")

            dbutils.fs.cp(f"{self.source}/{curr_file}", target_dir)
            self.curr_mo += 1

# COMMAND ----------

DA.init()

DA.paths.data_source = "/mnt/training/healthcare"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"

DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

# bronzePath             = f"{DA.paths.wokring_dir}/bronze"
# recordingsParsedPath   = f"{DA.paths.wokring_dir}/silver/recordings_parsed"
# recordingsEnrichedPath = f"{DA.paths.wokring_dir}/silver/recordings_enriched"
# dailyAvgPath           = f"{DA.paths.wokring_dir}/gold/daily_avg"

# checkpointPath               = f"{DA.paths.wokring_dir}/checkpoints"
#bronzeCheckpoint             = f"{DA.paths.checkpoints}/bronze"
# recordingsParsedCheckpoint   = f"{DA.paths.checkpoints}/recordings_parsed"
# recordingsEnrichedCheckpoint = f"{DA.paths.checkpoints}/recordings_enriched"
# dailyAvgCheckpoint           = f"{DA.paths.checkpoints}/dailyAvgPath"

DA.data_factory = DataFactory()
DA.conclude_setup()

# sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

