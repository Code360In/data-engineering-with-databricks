# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="jobs_demo_91"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def print_pipeline_config():
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 9.1.3 - DLT Job"

    displayHTML(f"""<table>
    <tr><td style="white-space:nowrap">Pipeline Name:</td><td><b>Jobs-Demo-91-{DA.username}</b></td></tr>
    <tr><td style="white-space:nowrap">Target:</td><td><b>{DA.db_name}</b></td></tr>
    <tr><td style="white-space:nowrap">Storage Location:</td><td><b>{DA.paths.working_dir}/storage</b></td></tr>
    <tr><td style="white-space:nowrap">Notebook Path:</td><td><b>{path}</b></td></tr>
    </table>""")
    
def print_job_config():
#     path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
#     path = "/".join(path.split("/")[:-1]) + "/DE 9.1.2 - Reset"
#     <tr><td style="white-space:nowrap">Notebook Path:</td><td><b>{path}</b></td></tr>
        
    displayHTML(f"""<table>
    <tr><td style="white-space:nowrap">Job Name:</td><td><b>Jobs-Demo-91-{DA.username}</b></td></tr>

    </table>""")    

# COMMAND ----------

DA.cleanup()
DA.init()
DA.data_factory = DltDataFactory()
DA.conclude_setup()

