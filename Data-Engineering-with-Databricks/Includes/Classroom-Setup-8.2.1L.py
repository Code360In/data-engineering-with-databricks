# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="dlt_lab_82"

# COMMAND ----------

# MAGIC %run ./mount-datasets

# COMMAND ----------

def get_pipeline_config():
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    path = "/".join(path.split("/")[:-1]) + "/DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab"
    
    pipeline_name = f"DLT-Lab-82L-{DA.username}"
    source = f"{DA.paths.working_dir}/source/tracker"
    return pipeline_name, path, source
    
def _print_pipeline_config():
    "Provided by DBAcademy, this function renders the configuration of the pipeline as HTML"
    pipeline_name, path, source = get_pipeline_config()
    
    displayHTML(f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{pipeline_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{DA.db_name}" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Storage Location:</td>
        <td><input type="text" value="{DA.paths.working_dir}/storage" style="width:100%"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Notebook Path:</td>
        <td><input type="text" value="{path}" style="width:100%"></td>
    </tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{source}" style="width:100%"></td>
    </tr>
    </table>""")
    
DA.print_pipeline_config = _print_pipeline_config    

# COMMAND ----------

def _create_pipeline():
    "Provided by DBAcademy, this function creates the prescribed pipline"
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    pipeline_name, path, source = get_pipeline_config()

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(pipeline_name)

    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = pipeline_name, 
        storage = f"{DA.paths.working_dir}/storage", 
        target = DA.db_name, 
        notebooks = [path],
        configuration = {"source": source})

    DA.pipline_id = pipeline.get("pipeline_id")
       
DA.create_pipeline = _create_pipeline

# COMMAND ----------

def _start_pipeline():
    "Provided by DBAcademy, this function starts the pipline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    # Start the pipeline
    start = client.pipelines().start_by_id(DA.pipline_id)
    update_id = start.get("update_id")

    # Get the status and block until it is done
    update = client.pipelines().get_update_by_id(DA.pipline_id, update_id)
    state = update.get("update").get("state")

    done = ["COMPLETED", "FAILED", "CANCELED"]
    while state not in done:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = client.pipelines().get_update_by_id(DA.pipline_id, update_id)
        state = update.get("update").get("state")
    
    print(f"The final state is {state}.")    
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

DA.start_pipeline = _start_pipeline    

# COMMAND ----------

DA.cleanup()
DA.init()

# DA.paths.data_source = "/mnt/training/healthcare"
# DA.paths.storage_location = f"{DA.paths.working_dir}/storage"
# DA.paths.data_landing_location    = f"{DA.paths.working_dir}/source/tracker"

DA.data_factory = DltDataFactory()
DA.conclude_setup()

