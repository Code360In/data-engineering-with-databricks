# Hands On Lab: Getting Started with the Databricks Platform

In this lab, you'll apply the skills you've just learned to:
- Create and manage an interactive cluster
- Import a Repo
- Create and clone a notebook
- Load a dataset into the DBFS
- Attach a notebook to a cluster and run a cell
- Review code changes in Repos

Before proceeding, ensure that you:
- Are logged into Databricks.
- Are in the **Data Science & Engineering Workspace** by selecting the appropriate entry from the persona menu.

## Create a Cluster

Running commands and processing data requires compute resources, which in Databricks is supplied by clusters. In this section, we will create a new interactive cluster which we will later use to run commands.

1. From the home page, select **New Cluster**, or click the **Compute** icon in the sidebar and click **Create Cluster**.
2. Specify *my_cluster* for **Cluster Name**.
3. Select **Single Node** for **Cluster Mode**. This will provide ample compute resources for this lab while minimizing compute costs.
4. Specify *60* to terminate the cluster after 60 minutes of inactivity. This more agressive value will further reduce compute costs.
5. Select the most recent version of the **Standard** runtime for **Databricks Runtime Version**.
6. Leave the remainder of the settings in their default states.
7. Click **Create Cluster**.

Cluster Creation will take several minutes. In the meantime, you may continue with the next section.

## Import a Repo

Databricks Repos is a feature that extends the concept of the Workspace to include revision control through its git integration. Repos also provides an API that enables integration with CI/CD tools.

In this section, we will create a Repo based on an existing repository on GitHub.

1. Click the **Repos** icon in the sidebar and click **Add Repo**.
2. With **Clone remote Git repo** selected, paste the following URL into the **Git repo URL** field:
   ```
   https://github.com/databricks-academy/platform-demos
   ```
   Notice that values for **Git provider** and **Repo name** are inferred. For this exercise, leave the inferred values as they are.
3. Click **Create**.

We now have a Repo called **platform-demos** that contains a copy of the materials from the underlying git repository. Though parallel to the folders of your Workspace, the user interface metaphors for Repos are identical and it is easy to move material back and forth as we will soon see.

## Create a Notebook

A Notebook is an interactive web document that allows use to run commands on our clusters in four different languages: Python, R, Scala and SQL. We can also intermix Markdown to embed structured documentation. In this section we will create a Notebook in the Workspace.

1. Click the **Workspace** icon in the sidebar.
2. Select the **Users** folder, which contains a personal folder for each user in the Workspace.
3. Identify the folder associated with your user id (this maps to the user id you use to login to Databricks). This is your personal folder for storing Databricks assets.
5. Create a new folder named `Home`:
    1. Click the chevron on the right side of your personal folder. Select **Create&rarr;Folder** from the menu.
    2. Specify *Home* for the folder name.
    3. Click **Create Folder**.
    4. Navigate to this new folder; select your personal folder to view the contents, then select **Home**.
6. Create a new Notebook named `my_name`:
    1. Click the chevron on the right side of the **Home** folder. Select **Create&rarr;Notebook** from the menu.
    2. Specify *my_name* for **Name**.
    3. Select **Python** as the **Default Language**.
    4. Select **my_cluster** for **Cluster**.
    5. Click **Create**. You now have a blank Notebook at the following path: */Users/&lt;user id&gt;/Home/my_name*. Let's begin adding some content to this Notebook.
7. Add a single line of Python code to the first cell of the Notebook. Substituting your own name on the right-hand side, this will assigne that value to a variable named `name`.
   ```
   name = "<your name>"
   ```

## Clone a Notebook

Let's return our attention to the Repo we created earlier. When setting up Repos, we often want to copy in Notebooks we have been developing in our Workspace. This process of copying Notebooks is referred to as cloning the Notebook. You can clone a Notebook from your Workspace to a different location in your Workspace, from your Workspace to a Repo, from a Repo to your Workspace, or even from one Repo to another Repo. In this section, we clone a Notebook to transfer it from ther Workspace into the Repo we created.

The cloning function is available from both the Workspace navigator, or from within the Notebook if you have it open (where it can be found in the **File** menu).

1. With the Notebook open, open the **File** menu and select **Clone**.
2. Replace the auto-generated value of **New Name** with *my_name*.
3. Specify the destination.
    1. Select **Repos** in the left-most column.
    2. Select the folder in the right column named after your user id.
    3. Select the **platform-demos** Repo that appears in the rightmost column.
    4. Click **Clone**.

The newly created copy of the Notebook opens. Note that there is now a Repo control button in the top-left because we are editing a Notebook contained in a Repo. The text appearing in this button reflects the branch of the Repo that is currently active; by default this will be either **main** or **master**, depending on how the underlying git repository was set up.

Provide instruction to clone the notebook they just created into the Repo that they imported in step 2 above.

(We'll want to be _very_ specific about the location; we'll create another notebook that uses a `%run` to call this notebook from a relative location and confirm success).

## Load a Dataset

NOTE: managing database tables, which we do in the next section, requires compute resources supplied by your running cluster. Let's ensure it's running now. If it isn't, let's restart it. This operation takes a few minutes and we can continue through this section while that completes.

1. Click the **Compute** icon in the sidebar.
2. Locate the cluster you created earlier. If the state is shown as **Terminated**, then restart it by clicking the **Start** button in the **Actions** column (these buttons only appear when hovering over the row).
3. The operation takes a couple of minutes, but you can complete this section in the meantime.

In this section we will upload a data file to DBFS using the Databricks UI. While this is a useful training exercise, this approach is not advised in a production scenario for two reasons:
- It's better to automate this, so that the entire environment can be replicated without human intervention
- It's not good practice to store your datasets in the DBFS root. Datasets should be stored in external storage that is mounted onto DBFS under */mnt*.

For this exercise, we use a sample data file we obtained from SimpleMaps (https://simplemaps.com/data/us-zips) that provides census and geographical information for each of the tens of thousands of US zip codes. Visit their website for more information or related products.

1. Download the data file. We provide a copy that can be directly downloaded from [here](./assets/uszips.csv).
2. In case you are operating in a shared environment where name conflicts may occur, rename the file using a pattern that's guaranteed to be unique. Use your full name (no spaces or other punctuation characters), but retain the *.csv* extension.
3. Upload the file to DBFS.
    1. Click the **Data** icon in the sidebar.
    2. Select **DBFS** to open the DBFS file browser.
    3. Click **Upload**.
    4. Leave the **DBFS Target Directory** blank; this will upload the file into the */FileStore* directory of DBFS.
    5. Click to use your operating system file browser to locate the file, or drag it from your local storage area to the drop zone. Either way, wait a few moments for it to upload.
    6. Once the upload completes, click **Done**.
4. (Optional) Navigate to the */Filestore* directory in the DBFS file navigator to validate that the file is present.

## Create a Table

With a data file loaded to DBFS, we will create a table based on it using the Databricks UI. Once again, this is a useful training exercise, but this approach is not advised in a production scenario for two reasons:
- It's better to automate this, so that the entire environment can be replicated without human intervention
- Particularly for large datasets, creating a Delta table will deliver superior performance. Creating an unmanaged table is also better since it decouples the table data and metadata. Neither of these options is available in the table creation UI.

1. Click the **Data** icon in the sidebar.
2. Select **Database Tables** to view and manage the databases and tables.
3. Click **Create Table**.
4. Select **DBFS** for **Data Source**, since the data file has been uploaded to DBFS already.
5. Navigate to and select the *.csv* file uploaded previously, located in the */FileStore* directory.
6. Click **Create Table with UI**.
7. Select your cluster from the **Cluster** dropdown.
8. Click **Preview Table**. Note the value for **Table Name** but leave it as is. You will need this value shortly.
9. Enable **First row is header** to promote the first row to be the column names.
10. Enable **Infer schema** to allow Spark to infer the data types for each column. Note that usage of this option is dependent on how you plan to use this table and whether you prefer all values to be treated as STRINGs in the table or if you would prefer the type to reflect the actual nature of the data in that column.
11. With the remainder of the options in their default states, click **Create Table**.

## Attach a Notebook to a Cluster

It is often necessary to attach a Notebook to a cluster. There are a number of reason this need might arise, including:
We need compute resources to run cells in our Notebooks, but maybe the Notebook is detached. This can happen for a few different reasons:
- Notebook was created without attaching to any cluster.
- The cluster originally attached to the Notebook was deleted.
- Need to test a Notebook against a different cluster configuration.

In this section, we will attach a Notebook from the Repo to our running cluster.

1. Click on the **Repos** icon in the sidebar.
2. Navigate to the **platform-demos** Repo.
3. Select the Notebook named **population_heatmap**. The Notebook opens.
4. Locate the cluster dropdown at the top-right corner. From the dropdown, select your cluster.

With a running cluster attached, we can now run cells within the Notebook.

## Run a Cell

Having attached a running cluster to our Notebook, let's run some cells.

1. Locate the first runnable cell in the Notebook, **Cmd 3**. This cell uses the `%run` *magic command* (code preceded with a percent sign) to source another Notebook named **my_name** (recall that was the Notebook we created earlier). This is a useful technique for modularizing code.
2. Run the cell using either of the following options:
    - Click the play button at the top-right corner of the cell, then select **Run Cell** from the menu.
    - With the cell selected, use the keyboard shortcut `Shift+Enter`.
   Because this cell runs a different Notebook, no output is captured. Instead, a summary of the execution time is provided.
3. Let's advance to the next runnable cell, **Cmd 5**. Note the following with regards to this cell:
    - It contains SQL, demonstrating that you can mix and match languages within a single Notebook
    - Because the default language for this Notebook is Python, this cell begins with the `%sql` magic command to denote that the cell is SQL.
    - It queries the table we created earlier, named in the Notebook **my_name**. This query builds a list of states with population counts for each.
4. Run the cell using one of the aforementioned methods.
    - Does it succeed?
    - If it did, you can advance to the next section.
    - If not, can you determine the reason it failed and fix it?
    - The remainder of this section walks through the fix.
5. Understand the root cause of the failure and how to fix it:
    - The query failed because of a mismatch between the table named in **my_name** and the actual name of the table.
    - Replace the name in **my_name** with the name of the table created earlier.
6. Locate and open the Notebook **my_name** within the Repo (*not* the copy in your Workspace).
7. Replace the value on the right-hand side of the assignment in **Cmd 1** with the table name noted earlier.
8. Return to the **population_heatmap** Notebook. Note, the **Recents** menu, accessible from the sidebar, is very handy for switching between Notebooks quickly.
9. Run the entire Notebook again by clicking **Run All**. This will ensure the updated name is pulled in prior to attempting the query again. This time, we are met with success.

## Review Changes in Repos

Let's see how our changes are reflected by the revision control capabilites that Repos provide.

1. Click the button at the top-right corner of the Notebook to open the **Repo** dialog.
2. Notice the itemized changes. Selecting each one in turn highlights what changed on the right-hand side of the dialog.
3. As write permissions are not available on the provided repository, you will not be able to commit and push. Instead, click **Close**.

One thing to note: nothing else we did in this lab is reflected in the changes:
- The *Home* directory we created
- The datasets we uploaded
- The first instance of the **my_name** Notebook

These changes were made outside the Repo and are hence not tracked. Notice also that other metadata (state, results, and comments, if any) are also not tracked.
