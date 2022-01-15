# Databricks notebook source
# MAGIC %run ./Student-Environment

# COMMAND ----------

# MAGIC %run ./Utilities-Datasets

# COMMAND ----------

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) >= 0
    except Exception:
        return False

def install_datasets(reinstall=False):
    working_dir = workingDirRoot
    course_name = "apache-spark-programming-with-databricks"
    version = "v01"
    min_time = "2 minute"
    max_time = "5 minutes"
  
    print(f"Your working directory is\n{working_dir}\n")

    # You can swap out the source_path with an alternate version during development
    # source_path = f"dbfs:/mnt/work-xxx/{course_name}"
    source_path = f"wasbs://courseware@dbacademy.blob.core.windows.net/{course_name}/{version}"
    print(f"The source for this dataset is\n{source_path}/\n")

    # Change the final directory to another name if you like, e.g. from "datasets" to "raw"
    target_path = f"{working_dir}/datasets"
    existing = path_exists(target_path)

    if not reinstall and existing:
        print(f"Skipping install of existing dataset to\n{target_path}")
        return 

    # Remove old versions of the previously installed datasets
    if existing:
        print(f"Removing previously installed datasets from\n{target_path}")
        dbutils.fs.rm(target_path, True)

    print(f"""Installing the datasets to {target_path}""")

    print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
          region that your workspace is in, this operation can take as little as {min_time} and 
          upwards to {max_time}, but this is a one-time operation.""")

    dbutils.fs.cp(source_path, target_path, True)
    print(f"""\nThe install of the datasets completed successfully.""")  
    
install_datasets(False)

