# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_demo_81"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def print_pipeline_config():
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 8.1.2 - SQL for Delta Live Tables"

    displayHTML(f"""<table>
    <tr><td style="white-space:nowrap">Pipeline Name:</td><td><b>DLT-Demo-81-{DA.username}</b></td></tr>
    <tr><td style="white-space:nowrap">Target:</td><td><b>{DA.db_name}</b></td></tr>
    <tr><td style="white-space:nowrap">Storage Location:</td><td><b>{DA.paths.storage_location}</b></td></tr>
    <tr><td style="white-space:nowrap">Notebook Path:</td><td><b>{path}</b></td></tr>
    </table>""")

# COMMAND ----------

DA.cleanup()
DA.init()

DA.paths.data_source = "/mnt/training/healthcare"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage"
DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

DA.data_factory = DltDataFactory()
DA.conclude_setup()

