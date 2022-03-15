# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_lab_82"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

# def print_sql(rows, sql):
#     displayHTML(f"""<body><textarea style="width:100%" rows={rows}> \n{sql.strip()}</textarea></body>""")
    

# COMMAND ----------

# generate_register_dlt_event_metrics_sql_string = ""

# def _generate_register_dlt_event_metrics_sql():
#     global generate_register_dlt_event_metrics_sql_string
    
#     generate_register_dlt_event_metrics_sql_string = f"""
# CREATE TABLE IF NOT EXISTS {DA.db_name}.dlt_events
# LOCATION '{DA.paths.working_dir}/storage/system/events';

# CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_success AS
# SELECT * FROM {DA.db_name}.dlt_events
# WHERE details:flow_progress:metrics IS NOT NULL;

# CREATE VIEW IF NOT EXISTS {DA.db_name}.dlt_metrics AS
# SELECT timestamp, origin.flow_name, details 
# FROM {DA.db_name}.dlt_success
# ORDER BY timestamp DESC;""".strip()
    
#     print_sql(13, generate_register_dlt_event_metrics_sql_string)
# DA.generate_register_dlt_event_metrics_sql = _generate_register_dlt_event_metrics_sql

# COMMAND ----------

# def _generate_daily_patient_avg():
#     sql = f"SELECT * FROM {DA.db_name}.daily_patient_avg"
#     print_sql(3, sql)

# DA.generate_daily_patient_avg = _generate_daily_patient_avg

# COMMAND ----------

# def _generate_visualization_query():
#     sql = f"""
# SELECT flow_name, timestamp, int(details:flow_progress:metrics:num_output_rows) num_output_rows
# FROM {DA.db_name}.dlt_metrics
# ORDER BY timestamp DESC;"""
    
#     print_sql(5, sql)

# DA.generate_visualization_query = _generate_visualization_query

# COMMAND ----------

def print_pipeline_config():
    displayHTML(f"""<table>
    <tr><td>Pipeline Name:</td><td><b>DLT-Lab-82L-{DA.username}</b></td></tr>
    <tr><td>Source:</td><td><b>{DA.paths.working_dir}/source/tracker</b></td></tr>
    <tr><td>Target:</td><td><b>{DA.db_name}</b></td></tr>
    <tr><td>Storage Location:</td><td><b>{DA.paths.working_dir}/storage</b></td></tr>
    </table>""")

# COMMAND ----------

DA.cleanup()
DA.init()

# DA.paths.data_source = "/mnt/training/healthcare"
# DA.paths.storage_location = f"{DA.paths.working_dir}/storage"
# DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

DA.data_factory = DltDataFactory()
DA.conclude_setup()

