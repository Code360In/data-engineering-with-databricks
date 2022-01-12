# Databricks notebook source
import pyspark.sql.functions as F
import re

class BuildEnvironmentVariables:

    def __init__(self, username):
        self.course_name = "eltsql"
        self.source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v01"

        self.username = username
        self.working_dir = f"dbfs:/user/{self.username}/dbacademy/{self.course_name}"
        self.userhome = self.working_dir # TEMPORARY BACKWARDS COMPATABILITY

        clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)
        self.database_name = f"{clean_username}_dbacademy_{self.course_name}"
        self.database_location = f"{self.working_dir}/db"
        
        self.source = f"{self.working_dir}/source_datasets"
        self.base_path=f"{self.working_dir}/tables"
        
        self.sales_table_path = f"{self.base_path}/sales"
        self.users_table_path = f"{self.base_path}/users"
        self.events_raw_table_path = f"{self.base_path}/events_raw"
        self.events_clean_table_path = f"{self.base_path}/events_clean"
        self.transactions_table_path = f"{self.base_path}/transactions"        
        self.clickpaths_table_path = f"{self.base_path}/clickpaths"
        
    def set_hive_variables(self):
        for (k, v) in self.__dict__.items():
            spark.sql(f"SET c.{k} = {v}")
  
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n")

# COMMAND ----------

username = spark.sql("SELECT current_user()").first()[0]
dbacademy_env = BuildEnvironmentVariables(username)
Paths = dbacademy_env # Temporary backwards compatability

# Hack for backwards compatability
username = dbacademy_env.username
database = dbacademy_env.database_name
userhome = dbacademy_env.working_dir

print(f"username: {username}")
print(f"database: {database}")
print(f"userhome: {userhome}")

# print(f"dbacademy_env:               Databricks Academy configuration object")
# print(f"dbacademy_env.username:      {dbacademy_env.username}")
# print(f"dbacademy_env.database_name: {dbacademy_env.database_name}")
# print(f"dbacademy_env.working_dir:   {dbacademy_env.working_dir}")

# COMMAND ----------

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) >= 0
    except Exception:
        return False

dbutils.widgets.text("mode", "default")
mode = dbutils.widgets.get("mode")

if mode == "reset" or mode == "cleanup":
    # Drop the database and remove all data for both reset and cleanup
    print(f"Removing the database {database}")    
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    
    print(f"Removing previously generated datasets from\n{dbacademy_env.working_dir}")    
    dbutils.fs.rm(dbacademy_env.working_dir, True)
    
if mode != "cleanup":
    # We are not cleaning up so we want to setup the environment
    
    # RESET is in case we want to force a reset
    # not-existing for net-new install
    if mode == "reset" or not path_exists(dbacademy_env.source): 
        print(f"\nInstalling datasets to\n{dbacademy_env.source}")
        print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the 
      region that your workspace is in, this operation can take as little as 3 minutes and 
      upwards to 6 minutes, but this is a one-time operation.""")
        
        dbutils.fs.cp(dbacademy_env.source_uri, dbacademy_env.source, True)
        print(f"""\nThe install of the datasets completed successfully.""") 

    # Create the database and use it.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbacademy_env.database_name} LOCATION '{dbacademy_env.database_location}'")
    spark.sql(f"USE {dbacademy_env.database_name}")
    
    # Once the database is created, init the hive variables
    dbacademy_env.set_hive_variables()

