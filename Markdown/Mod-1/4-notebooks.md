# Create and run Databricks notebooks

This demo will cover the creation and management of some simple Databricks Notebooks. By the end of this lesson you will be able to: 

* Create and populate your own multi-cell Notebook
* Incorporate visual elements into  your Notebooks
* Import and export Notebooks

Before proceeding, ensure that:
* You have a cluster running as it will be needed to run commands
* Have a sample data file for reading. The code example here assumes the existence of **/FileStore/uszips.csv** in DBFS, as per the DBFS demo

## Create Notebook

Create a new Notebook. Highlight that because it's such a common thing to do, there are a few ways, including:
* From the home page
* From the **Create** menu
* From any folder item in the Workspace for which you have appropriate permissions (click the chevron, then **Create&rarr;Notebook**)

When choosing a name, be sure to mention that you can easily change the name afterward.

When selecting default language, mention that since Notebooks are essentially a collection of commands, you should choose the language you anticipate using the most. That said, there's no need to overthink this since it too can be changed after, and individual cells can also override this choice. For the sake of an example, choose **Python**.

For cluster, choose a running cluster for which you have permissions. These compute resources will be needed when you go to execute commands in the Notebook.

## Create a Markdown cell

Start by documenting the Notebook with a bit of text.

    %md
    # Example Notebook
    This is a description of the Notebook

Take a moment to talk about the *magic command* in the first line. By default, Databricks will assume this cell contains Python (the default language). The magic command overrides this. Databricks supports a few different magic commands for such designatations and various other tasks.

Draw attention to the syntax highlight as you type, which start to become very handing when writing SQL, Python or others.

Add another Markdown cell with the following contents.

    %md
    ## Read a data file
    Read and display a CSV from DBFS.

Open the table of contents panel and illustrate how thoughtful application of Markdown can make your Notebooks more easy to read and navigate.

## Create a Python cell

Now add another cell containing the following Python code:

    df = spark.read.csv("/FileStore/uszips.csv",
                        header="true", 
                        inferSchema="true")
    display(df)

Draw attention to the fact that we don't need a magic command for this cell because Python is the default language for the Notebook.

Here we are leveraging the automatically created SparkSession object, **spark**, to read the CSV file located in the DBFS **FileStore** directory into a **DataFrame** object named `df`. The parameters intercept the header row, and causes the schema to be inferred from sapling the input data.

Then we display the data using the Databricks `display()` function. Databricks formats this output as a table by default. Highlight that you can scroll through the data, and sort it by clicking on the headers. 

Add another cell with the code:

    df.createOrReplaceTempView('temp')
    
This code creates a *temporary view*, that is, a database table local to this Notebook, based on the data referred to by the `df` object. Running it gives us the opportunity to reinforce some important points:
* execution state is maintained across cells in a Notebook (per language). In this cell we can seamlessly refer to `df` that was created in a different cell.
* no output is generated here because we didn't call `display()`. It's not necessary anyway since there's nothing new to display; we're simply creating an alternate representation of data we already read in.

## Create an SQL cell

Add another cell:

    %sql
    SELECT * FROM temp

A few things of note:
* here we need a magic command since we're departing from the default language. 
* we are simply querying all the data from a table named **temp**; that is, the temporary view we created in the previous cell from Python code
* temporary views can be used to create alternate representations of data that you don't want to make globablly available; they're also useful for querying DataFrames from SQL
* Without having to do anything extra, Databricks is displaying the output in a table
* the data is identical to the earlier cell (as it should be, since it's the same data; the only thing that has changed is how it's being stored)

## Add Visualizations

Create a new cell with the following:

    %sql
    SELECT `state_id` AS `state`,COUNT(`zip`) AS `nzips`
    FROM temp
    WHERE `state_id` NOT IN ('AS','GU','MP','PR','VI') 
    GROUP BY `state` 
    ORDER BY `nzips`

This more elaborate query pulls only the **state_id** and **zip** columns, and aggregates by **zip**. It's also filtering out some states and sorting the output by the count. The end result is a record per state with the count of zip codes in that state. Run the cell. Once you get a table output, click the bar graph plot. Let’s run the query to see the output. This gives us a 51-row table, as expected, counting the number of reported zip codes for each state. Click the bar graph to switch visualiations. Highlight how you can customize these and, ultimately using DBSQL, could pull these graphics into a dashboard.

For another great visual, add the following cell:

    %sql
    SELECT `state_id` AS `state`,SUM(`population`) AS `population`
    FROM temp
    WHERE `state_id` NOT IN ('AS','GU','MP','PR','VI')
    GROUP BY `state`

Run and select the **Map** plot. This gives us a heatmap presenting the population per state.

## Export and Import Notebooks

You can share Notebooks by exporting them. Open **File&rarr;Export**, and highlight 3 important options:
* **DBC Archive**: an archive that can be easily imported into another Workspace. This includes state and results
* **Source File**: a source code file (in the syntax of the default language) representing the text/code in the Notebook (no state or results are captured)
* **HTML**: a static web page capturing the Notebook (and results), useful for publishing or documentation

Export the Notebook using HTML, then open the resulting .html file in a web browser. Highlight that the visuals are all included and as interactive as they were in the Notebook.

Now export as a DBC Archive.

** Import Notebooks

Let's highlight two ways to import. First create a folder in your home directory in the Workspace. Call it **Test**. In that folder, select the import option. Now drag the .dbc file downloaded previously to the shared area of the dialog. This highlights the file import method (you can also browse for the file rather than dropping it onto the dialog). This gives us a copy of the Notebook we exported previously.

Now go back to the **Test** folder and import again. This time use the URL option and paste the link:

    https://docs.databricks.com/_static/notebooks/delta/quickstart-python.html
    
## Comments

Like many online document collaboration platforms, Notebooks support comments. Highlight some code, click the icon and add a comment. As you might expect, you can edit or reply to comments. 

## Revision history

Notebooks also have built-in revision history. Though not as powerful as an external revisioin control system like git, it does provide some basic abilities to inspect the history of a Notebook, what changed when, by whom, etc. Git integration through Databricks Repos, which we discuss shortly, provide additional capabilities.
