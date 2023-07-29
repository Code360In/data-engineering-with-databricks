# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Managing Permissions for Databases, Tables, and Views
# MAGIC 
# MAGIC The instructions as detailed below are provided for groups of users to explore how Table ACLs on Databricks work. It leverages Databricks SQL and the Data Explorer to accomplish these tasks, and assumes that at least one user in the group has administrator status (or that an admin has previously configured permissions to allow proper permissions for users to create databases, tables, and views). 
# MAGIC 
# MAGIC As written, these instructions are for the admin user to complete. The following notebook will have a similar exercise for users to complete in pairs.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the default permissions for users and admins in DBSQL
# MAGIC * Identify the default owner for databases, tables, and views created in DBSQL and change ownership
# MAGIC * Use Data Explorer to navigate relational entities
# MAGIC * Configure permissions for tables and views with Data Explorer
# MAGIC * Configure minimal permissions to allow for table discovery and querying

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Generate Setup Statements
# MAGIC 
# MAGIC The following cell uses Python to extract username of the current user and format this into several statements used to create databases, tables, and views.
# MAGIC 
# MAGIC Only the admin needs to execute the following cell. Successful execution will print out a series of formatted SQL queries, which can be copied into the DBSQL query editor and executed.

# COMMAND ----------

DA.generate_users_table()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Run the cell above
# MAGIC 1. Copy the entire output to your clipboard
# MAGIC 1. Navigate to the Databricks SQL workspace
# MAGIC 1. Make sure that a DBSQL endpoint is running
# MAGIC 1. Use the left sidebar to select the **SQL Editor**
# MAGIC 1. Paste the query above and click the blue **Run** in the top right
# MAGIC 
# MAGIC **NOTE**: You will need to be connected to a DBSQL endpoint to execute these queries successfully. If you cannot connect to a DBSQL endpoint, you will need to contact your administrator to give you access.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Using Data Explorer
# MAGIC 
# MAGIC * Use the left sidebar navigator to select the **Data** tab; this places you in the **Data Explorer**
# MAGIC 
# MAGIC ## What is the Data Explorer?
# MAGIC 
# MAGIC The data explorer allows users and admins to:
# MAGIC * Navigate databases, tables, and views
# MAGIC * Explore data schema, metadata, and history
# MAGIC * Set and modify permissions of relational entities
# MAGIC 
# MAGIC Note that at the moment these instructions are being written, Unity Catalog is not yet generally available. The 3 tier namespacing functionality it adds can be previewed to an extent by switching between the default **`hive_metastore`** and the **`sample`** catalog used for example dashboards and queries. Expect the Data Explorer UI and functionality to evolve as Unity Catalog is added to workspaces.
# MAGIC 
# MAGIC ## Configuring Permissions
# MAGIC 
# MAGIC By default, admins will have the ability to view all objects registered to the metastore and will be able to control permissions for other users in the workspace. Users will default to having **no** permissions on anything registered to the metastore, other than objects that they create in DBSQL; note that before users can create any databases, tables, or views, they must have create and usage privileges specifically granted to them.
# MAGIC 
# MAGIC Generally, permissions will be set using Groups that have been configured by an administrator, often by importing organizational structures from SCIM integration with a different identity provider. This lesson will explore Access Control Lists (ACLs) used to control permissions, but will use individuals rather than groups.
# MAGIC 
# MAGIC ## Table ACLs
# MAGIC 
# MAGIC Databricks allows you to configure permissions for the following objects:
# MAGIC 
# MAGIC | Object | Scope |
# MAGIC | --- | --- |
# MAGIC | CATALOG | controls access to the entire data catalog. |
# MAGIC | DATABASE | controls access to a database. |
# MAGIC | TABLE | controls access to a managed or external table. |
# MAGIC | VIEW | controls access to SQL views. |
# MAGIC | FUNCTION | controls access to a named function. |
# MAGIC | ANY FILE | controls access to the underlying filesystem. Users granted access to ANY FILE can bypass the restrictions put on the catalog, databases, tables, and views by reading from the file system directly. |
# MAGIC 
# MAGIC **NOTE**: At present, the **`ANY FILE`** object cannot be set from Data Explorer.
# MAGIC 
# MAGIC ## Granting Privileges
# MAGIC 
# MAGIC Databricks admins and object owners can grant privileges according to the following rules:
# MAGIC 
# MAGIC | Role | Can grant access privileges for |
# MAGIC | --- | --- |
# MAGIC | Databricks administrator | All objects in the catalog and the underlying filesystem. |
# MAGIC | Catalog owner | All objects in the catalog. |
# MAGIC | Database owner | All objects in the database. |
# MAGIC | Table owner | Only the table (similar options for views and functions). |
# MAGIC 
# MAGIC **NOTE**: At present, Data Explorer can only be used to modify ownership of databases, tables, and views. Catalog permissions can be set interactively with the SQL Query Editor.
# MAGIC 
# MAGIC ## Privileges
# MAGIC 
# MAGIC The following privileges can be configured in Data Explorer:
# MAGIC 
# MAGIC | Privilege | Ability |
# MAGIC | --- | --- |
# MAGIC | ALL PRIVILEGES | gives all privileges (is translated into all the below privileges). |
# MAGIC | SELECT | gives read access to an object. |
# MAGIC | MODIFY | gives ability to add, delete, and modify data to or from an object. |
# MAGIC | READ_METADATA | gives ability to view an object and its metadata. |
# MAGIC | USAGE | does not give any abilities, but is an additional requirement to perform any action on a database object. |
# MAGIC | CREATE | gives ability to create an object (for example, a table in a database). |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Review the Default Permissions
# MAGIC In the Data Explorer, find the database you created earlier (this should follow the pattern **`dbacademy_<username>_acls_demo`**).
# MAGIC 
# MAGIC Clicking on the database name should display a list of the contained tables and views on the left hand side. On the right, you'll see some details about the database, including the **Owner** and **Location**. 
# MAGIC 
# MAGIC Click the **Permissions** tab to review who presently has permissions (depending on your workspace configuration, some permissions may have been inherited from settings on the catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Assigning Ownership
# MAGIC 
# MAGIC Click the blue pencil next to the **Owner** field. Note that an owner can be set as an individual OR a group. For most implementations, having one or several small groups of trusted power users as owners will limit admin access to important datasets while ensuring that a single user does not create a choke point in productivity.
# MAGIC 
# MAGIC Here, we'll set the owner to **Admins**, which is a default group containing all workspace administrators.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Change Database Permissions
# MAGIC 
# MAGIC Begin by allowing all users to review metadata about the database.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Make sure you have the **Permissions** tab selected for the database
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **USAGE** and **READ_METADATA** options
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**
# MAGIC 
# MAGIC Note that users may need to refresh their view to see these permissions updated. Updates should be reflected for users in near real time for both the Data Explorer and the SQL Editor.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Change View Permissions
# MAGIC 
# MAGIC While users can now see information about this database, they won't be able to interact with the table of view declared above.
# MAGIC 
# MAGIC Let's start by giving users the ability to query our view.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Select the **`ny_users_vw`**
# MAGIC 1. Select the **Permissions** tab
# MAGIC    * Users should have inherited the permissions granted at the database level; you'll be able to see which permissions users currently have on an asset, as well as where that permission is inherited from
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **SELECT** and **READ_METADATA** options
# MAGIC    * **READ_METADATA** is technically redundant, as users have already inherited this from the database. However, granting it at the view level allows us to ensure users still have this permission even if the database permissions are revoked
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run a Query to Confirm
# MAGIC 
# MAGIC In the **SQL Editor**, all users should use the **Schema Browser** on the lefthand side to navigate to the database being controlled by the admin.
# MAGIC 
# MAGIC Users should start a query by typing **`SELECT * FROM`** and then click the **>>** that appears while hovering over the view name to insert it into their query.
# MAGIC 
# MAGIC This query should return 2 results.
# MAGIC 
# MAGIC **NOTE**: This view is defined against the **`users`** table, which has not had any permissions set yet. Note that users have access only to that portion of the data that passes through the filters defined on the view; this pattern demonstrates how a single underlying table can be used to drive controlled access to data for relevant stakeholders.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Change Table Permissions
# MAGIC 
# MAGIC Perform the same steps as above, but now for the **`users`** table.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Select the **`users`** table
# MAGIC 1. Select the **Permissions** tab
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **SELECT** and **READ_METADATA** options
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Have Users Attempt to **`DROP TABLE`**
# MAGIC 
# MAGIC In the **SQL Editor**, encourage users to explore the data in this table.
# MAGIC 
# MAGIC Encourage users to try to modify the data here; assuming permissions were set correctly, these commands should error out.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Create a Database for Derivative Datasets
# MAGIC 
# MAGIC In most cases users will need a location to save out derivative datasets. At present, users may not have the ability to create new tables in any location (depending on existing ACLs in the workspace and databases created during previous lessons students have completed).
# MAGIC 
# MAGIC The cell below prints out the code to generate a new database and grant permissions to all users.
# MAGIC 
# MAGIC **NOTE**: Here we set permissions using the SQL Editor rather than the Data Explorer. You can review the Query History to note that all of our previous permission changes from Data Explorer were executed as SQL queries and logged here (additionally, most actions in the Data Explorer are logged with the corresponding SQL query used to populate the UI fields).

# COMMAND ----------

DA.generate_create_database_with_grants()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Have Users Create New Tables or Views
# MAGIC 
# MAGIC Give users a moment to test that they can create tables and views in this new database.
# MAGIC 
# MAGIC **NOTE**: because users were also granted **MODIFY** and **SELECT** permissions, all users will immediately be able to query and modify entities created by their peers.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Admin Configuration
# MAGIC 
# MAGIC At present, users do not have any Table ACL permissions granted on the default catalog **`hive_metastore`** by default. The next lab assumes that users will be able to create databases.
# MAGIC 
# MAGIC To enable the ability to create databases and tables in the default catalog using Databricks SQL, have a workspace admin run the following command in the DBSQL query editor:
# MAGIC 
# MAGIC <strong><code>GRANT usage, create ON CATALOG &#x60;hive_metastore&#x60; TO &#x60;users&#x60;</code></strong>
# MAGIC 
# MAGIC To confirm this has run successfully, execute the following query:
# MAGIC 
# MAGIC <strong><code>SHOW GRANT ON CATALOG &#x60;hive_metastore&#x60;</code></strong>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
