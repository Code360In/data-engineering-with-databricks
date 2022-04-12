# Databricks notebook source
# MAGIC %run ./_utility-methods $lesson="11.1"

# COMMAND ----------

def print_sql(rows, sql):
    html = f"""<textarea style="width:100%" rows="{rows}"> \n{sql.strip()}</textarea>"""
    displayHTML(html)

# COMMAND ----------

def _generate_users_table():
    print_sql(20, f"""
CREATE DATABASE IF NOT EXISTS {DA.db_name}
LOCATION '{DA.paths.user_db}';

USE {DA.db_name};

CREATE TABLE users (id INT, name STRING, value DOUBLE, state STRING);

INSERT INTO users
VALUES (1, "Yve", 1.0, "CA"),
       (2, "Omar", 2.5, "NY"),
       (3, "Elia", 3.3, "OH"),
       (4, "Rebecca", 4.7, "TX"),
       (5, "Ameena", 5.3, "CA"),
       (6, "Ling", 6.6, "NY"),
       (7, "Pedro", 7.1, "KY");

CREATE VIEW ny_users_vw
AS SELECT * FROM users WHERE state = 'NY';
""")
    
DA.generate_users_table = _generate_users_table

# COMMAND ----------

def _generate_create_database_with_grants():
    print_sql(7, f"""
CREATE DATABASE {DA.db_name}_derivative;

GRANT USAGE, READ_METADATA, CREATE, MODIFY, SELECT ON DATABASE `{DA.db_name}_derivative` TO `users`;

SHOW GRANT ON DATABASE `{DA.db_name}_derivative`;""")    
    
DA.generate_create_database_with_grants = _generate_create_database_with_grants

# COMMAND ----------

DA.cleanup()
DA.init(create_db=False)
DA.conclude_setup()

