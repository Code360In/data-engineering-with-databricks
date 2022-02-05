# Databricks notebook source
def install_source_dataset(source_uri, reset, subdir):
    target_dir = f"{DA.working_dir_prefix}/source/{subdir}"

    if reset and DA.paths.exists(target_dir):
        dbutils.fs.rm(target_dir, True)
    
    if DA.paths.exists(target_dir):
        print(f"Skipping install to \"{target_dir}\", dataset already exists")
    else:
        print(f"Installing datasets to \"{target_dir}\"")
        dbutils.fs.cp(source_uri, target_dir, True)
        
    return target_dir

# COMMAND ----------

def install_dtavod_datasets(reset=False):
    source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/databases_tables_and_views_on_databricks/v02"
    DA.paths.datasets = install_source_dataset(source_uri, reset, "dtavod")


# COMMAND ----------

def install_eltwss_datasets(reset=False):
    source_uri = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v02/small-datasets"
    DA.paths.datasets = install_source_dataset(source_uri, reset, "eltwss")

# COMMAND ----------

def clone_source_table(table_name, source_path, source_name=None):
    import time
    start = int(time.time())

    source_name = table_name if source_name is None else source_name
    print(f"Creating the {table_name} table from {source_path}/{source_name}", end="...")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name}
        SHALLOW CLONE delta.`{source_path}/{source_name}`
        """)

    total = spark.read.table(table_name).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

def copy_source_dataset(src_path, dst_path, format, name):
    import time
    start = int(time.time())
    print(f"Creating the {name} dataset", end="...")
    
    dbutils.fs.cp(src_path, dst_path, True)

    total = spark.read.format(format).load(dst_path).count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

def load_eltwss_tables():
    clone_source_table("events", f"{DA.paths.datasets}/delta")
    clone_source_table("sales", f"{DA.paths.datasets}/delta")
    clone_source_table("users", f"{DA.paths.datasets}/delta")
    clone_source_table("transactions", f"{DA.paths.datasets}/delta")

# COMMAND ----------

def load_eltwss_external_tables():
    copy_source_dataset(f"{DA.paths.datasets}/raw/sales-csv", f"{DA.paths.working_dir}/sales-csv", "csv", "sales-csv")

    import time
    start = int(time.time())
    print(f"Creating the users table", end="...")

    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    (spark.read
          .format("parquet")
          .load(f"{DA.paths.datasets}/raw/users-historical")
          .repartition(1)
          .write
          .format("org.apache.spark.sql.jdbc")
          .option("url", f"jdbc:sqlite:/{DA.username}_ecommerce.db")
          .option("dbtable", "users") # The table name in sqllight
          .mode("overwrite")
          .save())

    total = spark.read.parquet(f"{DA.paths.datasets}/raw/users-historical").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

# COMMAND ----------

# lesson: Writing delta 
def create_eltwss_users_update():
    import time
    start = int(time.time())
    print(f"Creating the users_dirty table", end="...")

    spark.sql(f"""
        CREATE OR REPLACE TABLE users_dirty AS
        SELECT *, current_timestamp() updated 
        FROM parquet.`{DA.paths.datasets}/raw/users-30m`
    """)
    
    spark.sql("INSERT INTO users_dirty VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)")
    
    total = spark.read.table("users_dirty").count()
    print(f"({int(time.time())-start} seconds / {total:,} records)")

