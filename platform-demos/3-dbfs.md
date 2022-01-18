# Manage data with DBFS

This demo will explore DBFS from the Databricks Data Science and Engineering Workspace. By the end of this lesson you will be able to: 

* Describe and navigate the Databricks File System (DBFS)
* Import files into DBFS

Before proceeding, ensure that:
* DBFS browsing functionality is enabled in your Workspace (**Settings&rarr;Admin Console&rarr;Workspace Settings&rarr;Advanced&rarr;DBFS File Browser**)
* There's a running cluster you can attach to. For basic DBFS browsing from the Workspace, this isn't a strict requirement &ndash; but it will be good to have one running to give a better demonstration.
* Have a sample data file on hand for upload. Suggest using uszips.csv, which can be downloaded from here: https://simplemaps.com/data/us-zips. The file contains census and geographical information for each of the tens of thousands of US zip codes. Displaying the website in class satisfies the licensing requirements for using this sample dataset.

## Explore DBFS

Go to the **Data** page, where we can access DBFS. Mention that DBFS browsing is a feature that can be turned on or off by the administrator (can show this if desired).

What we see here represents the *DBFS root* &ndash; that is, the top-level directory within DBFS. The DBFS root is backed by a storage bucket that was set up by your Databricks platform administrator.

Explain some of the special folders:

* /Filestore: this multi-purpose directory stores files uploaded to DBFS from the Workspace, such as data files, images and libraries. Output files (like images created by Notebook visualizations) are also stored here, from where they can be downloaded.
* /databricks-datasets (not visible in the browser as it is mounted and not physically copied): this directory contains example datasets provided by Databricks to accompany product documentation. See https://docs.databricks.com/data/databricks-datasets.html for more information.
* /databricks-results: this directory contains the full results of your queries. These files are generated when you download the full results of a query that returns more than the 1000 rows displayed by default.
* /databricks-init (only visible if set up): this directory is used to host initialization scripts that are executed on each node of a starting cluster prior to invoking the JVM environment for the driver or worker.
* /user/hive/warehouse: also known as the Databricks Hive metastore, this directory contains metadata related to Databricks tables and, in general, the table data itself. Note that Databricks is capable of using an external Hive metastore if needed.
* /mnt: this directory contains object storage mount points
* /tmp: this directory provides a space to store temporary working files

Also mention the following general points regarding DBFS:
* All nodes in your clusters will be able to see everything we see here in the Workspace
* Don't confuse DBFS with the filesystem-like organization of the Workspace (feel free to open the **Workspace** page as a reminder). The two are unrelated.
* DBFS does not represent the local file system running on your cluster nodes; each node has its own local file system into which DBFS is mounted.

Use the browser to explore some of these. **/FileStore** and **/usr/hive** are good examples. Also look at **/mnt** if attached to a running cluster, which is where external data sources are mounted by convention. Remind them that the directories inside **/mnt** are not "real" directories, but rather *mountpoints* (that is, a virtual directory that redirects to an external filesystem-like resources). Because they require some degree of processing, we must be attached to a running cluster to see them.

## Cluster view of DBFS

To help reinforce these points about DBFS, demo the following in a new Python Notebook. And though we get more into Notebooks in the next lesson, for now just say enough: that Notebooks provide an interactive environmnet for running code on our clusters.

Create and execute a cell with the following:

    display(dbutils.fs.ls('/'))
    
This is using the DBFS utility class to get a list of entries in the top-level directory of DBFS, then displaying them with the multi-purpose Databricks `display()` function. Raise the following points:
* We see some entries (like **databricks-datasets**) here but not in the Workspace browser

Create a new cell and contrast the results from above with the following:

    import os
    print(os.listdir('/'))

This is using the standard os utility class to get a list of entries in the top-level directory of the cluster's local file system. Raise the following points:
* there are a collection of standard Linux file system directories and files found here, because we are listing the contents of the local file system, not DBFS
* there is a **/dbfs** directory, where DBFS is actually mounted.

## Upload from Workspace

Let’s upload a file to DBFS.

With the DBFS tab in the Data page selected, click the **Upload** button. Browse to locate the .csv file (contained in the .zip file downloaded from above), or drag the file to the shared area of the dialog.

Call out that the file will be uploaded into the **/FileStore** directory. We could create additional sub-directories here if desired, but when uploading through he UI it's always going to be somewhere under **FileStore**.

Show that it's there by navigating to the **/FileStore** in the Workspace. Can also re-run the Notebook created earlier to check the file's presence.

Bring up the point that this example file represents a relatively small dataset. For production environments:
* uploading large datasets into DBFS is generally not recommended. In that case, the preferred approach is to connect, or mount, an external data source onto DBFS and maintain your data there.
* uploading via the user interface is not recommended. Consider automating this using the CLI or API

## File Management

Call out to the prefix searching capabilities of the UI to help you find files as your DBFS scales.

Also demonstrate usage of the chevron element on each item that allows us to perform some additional operations. For the file we just uploaded, we could move, rename or delete it as needed.
