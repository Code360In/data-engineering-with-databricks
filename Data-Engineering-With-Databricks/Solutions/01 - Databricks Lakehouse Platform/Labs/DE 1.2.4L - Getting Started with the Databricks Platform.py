# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started with the Databricks Platform
# MAGIC 
# MAGIC This notebook provides a hands-on review of some of the basic functionality of the Databricks Data Science and Engineering Workspace.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lessons, you will be able to:
# MAGIC - Rename a Notebook and change the default language
# MAGIC - Attach a cluster
# MAGIC - Use the %run magic command
# MAGIC - Run Python and SQL cells
# MAGIC - Create a Markdown cell

# COMMAND ----------

# MAGIC %md
# MAGIC # Renaming a Notebook
# MAGIC 
# MAGIC Changing the name of a Notebook is easy. Click on the name at the top of this page, then make changes to the name. To make it easier to navigate back to this Notebook later in case you need to, append a short test string to the end of the existing name.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Attaching a cluster
# MAGIC 
# MAGIC Executing cells in a Notebook requires computing resources, which is provided by clusters. The first time you execute a cell in a Notebook, you will be prompted to attach to a cluster if one is not alread attached.
# MAGIC 
# MAGIC Attach a cluster to this Notebook now by clicking the dropdown near the top-left corner of this page. Select the cluster you created previously. This will clear the execution state of the Notebook and connect the Notebook to the selected cluster.
# MAGIC 
# MAGIC Note that the dropdown menu provides the option of starting or restarting the cluster as needed. You can also detach and re-attach to a cluster in a single movement. This is useful for clearing the execution state when needed.

# COMMAND ----------

# MAGIC %md
# MAGIC # Using %run
# MAGIC 
# MAGIC Complex projects of any type can benefit from the ability to break them down into simpler, reusable components.
# MAGIC 
# MAGIC In the context of Databricks Notebooks, this facility is provided through the `%run` magic command, which allows you to execute Notebooks within Notebooks, as though all the executable cells of the target Notebook were inlined into the calling Notebook.
# MAGIC 
# MAGIC The folder that contains this Notebook constains a subfolder named `Includes`, which in turn contains a Notebook called `example-setup`. This simple Notebook creates a DataFrame called `exampleDF`. 
# MAGIC 
# MAGIC Recalling that this subfolder can be indexed using `./Includes`, fill in the cell below with code that will execute this Notebook. Once you are done, test the cell by running it.

# COMMAND ----------

# ANSWER
%run ./Includes/example-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a Python cell
# MAGIC 
# MAGIC Run the following cell to verify that the `example-setup` Notebook was executed by displaying the `exampleDF` Dataframe. This table consists of 16 rows of increasing values.

# COMMAND ----------

display(exampleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Detach and Reattach a Cluster
# MAGIC 
# MAGIC While attaching to clusters is a fairly common task, sometimes it is useful to detach and re-attach in one single operation. The main side-effect this achieves is clearing the execution state. This can be useful when you want to test cells in isolation, or you simply want to reset the execution state.
# MAGIC 
# MAGIC Revisit the cluster dropdown. In the menu item representing the currently attached cluster, select the **Detach & Re-attach** link.
# MAGIC 
# MAGIC Notice that the output from the cell above remains since results and execution state are unrelated, but the execution state is cleared. This can be verified by attempting to re-run the cell above. This fails, since the `examplesDF` variable has been cleared, along with the rest of the state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Change Language
# MAGIC 
# MAGIC Notice that the default language for this Notebook is set to Python. Change this by clicking the **Python** button to the right of the Notebook name. Change the default language to SQL.
# MAGIC 
# MAGIC Notice that the Python cells are automatically prepended with a <strong><code>&#37;python</code></strong> magic command to maintain validity of those cells. Notice that this operation also clears the execution state.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Markdown Cell
# MAGIC 
# MAGIC Add a new cell below this one. Populate with some Markdown that includes at least the following elements:
# MAGIC * A header
# MAGIC * Bullet points
# MAGIC * A link (using your choice of HTML or Markdown conventions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a SQL cell
# MAGIC 
# MAGIC Run the following cell to query a Delta table using SQL. This executes a simple query against a table is backed by a Databricks-provided example dataset included in all DBFS installations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled`

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the following cell to view the underlying files backing this table.

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Review Changes
# MAGIC 
# MAGIC Assuming you have imported this material into your workspace using a Databricks Repo, open the Repo dialog by clicking the button at the top-right corner of this page. Use the dialog to revert the changes and restore this Notebook to its original state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC By completing this lab, you should now feel comfortable manipulating Notebooks, creating new cells, and running Notebooks within Notebooks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
