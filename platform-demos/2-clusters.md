# Create and manage interactive clusters

This demo will cover creating and managing all-purpose Databricks clusters using the Databricks Data Science and Engineering Workspace. By the end of this lesson you will be able to:
* Describe cluster structures
* Create and manage clusters

## Create Cluster

Create a new cluster by using the link in the **Common Tasks** section of the home page, or by going to the **Compute** page and clicking **Create Cluster**. In either case, be sure to mention that there are a couple ways to do this, like many common tasks.

Discuss the options. Be sure to mention at least the following:
* **Cluster mode**: as mentioned in the slides, there are three different modes. For the small workloads like the ones we use in the training, you can use the **Single Node** option here, but note that this disables some other options. For more info, see https://docs.databricks.com/clusters/configure.html#cluster-mode 
* **Databricks Runtime Version**: this specifies the software to deploy to the clusters.
  * Highlight the different familes to choose from (**Standard** vs **ML** for machine learning workloads)
  * Generally we choose the latest version though do mention that the versions marked "LTS" carry extended support which might be a preferential option for some production systems. Refer to https://docs.databricks.com/release-notes/runtime/databricks-runtime-ver.html#runtime-support for more info.
  * The **Photon** checkbox filters versions that support Databricks' new vectorized query engine. For more info, see https://docs.databricks.com/runtime/photon.html.
  For general info on Databricks Runtime, refer to https://docs.databricks.com/runtime/dbr.html, 
* **Enable autoscaling** allows the dynamic adjustment (up or down) to the number of worker nodes through a range (**Min Workers** and **Max Worker**) based on workload. Note that this option is not available when **Cluster Mode** is set to **Single Node**
* **Enable autoscaling local storage** allows the additional EBS volumes to be dynamically attached if disk space is getting low (increase only). See https://docs.databricks.com/clusters/configure.html#autoscaling-local-storage for more deteails.
* **Terminate after** to control compute costs by automatically terminating clusters after a period of inactivity (that is, absence of Spark jobs)
* **Worker Type** to specify the cloud-specfic VM profile for the worker nodes, determining the CPU/RAM configuration for each. Note that this option is not available when **Cluster Mode** is set to **Single Node**
* **Workers** to specify the number of worker nodes. Note that this option is not available when **Cluster Mode** is set to **Single Node**, or the **Enable autoscaling** option is selected
* **Min Workers** and **Max Workers** to specify the minimum and maximum number of worker nodes. Note that these are only available when **Enable autoscaling** option is selected.
* **Driver Type** (or **Node Type** when **Cluster Mode** is set to **Single Node**) specifies the cloud-specfic VM profile for the driver node, determining the CPU/RAM configuration.

With respect to the Worker/Driver/Node Type, here are some general recommendations:
* Memory optimized (eg: r4, DSv2): Memory-intensive applications
* Compute optimized (eg: c5, Fsv2): Structured Streaming, Distributed Analytics, Data Science Applications
* Storage optimized (eg: i3, Lsv2): Applications that require higher disk throughput and IO
* General purpose (eg: m4, DSv2): Enterprise-grade applications, relational databases, and analytics with in-memory caching

Highlight the DBU estimation. Adjust settings (like Cluster Mode, Workers/Min Workers/Max Workers, and Worker and Driver Types) to see how it impacts this estimate.

Open the **Advanced Options** to show what's there, but we won't go into details at this point.

Create the cluster. While the cluster spins up, it's an opportune moment to talk about the merits of using Pools. You can demonstrate the workflow quickly by:
1. Visiting the **Pools** tab
2. Create a pool with all the default settings
3. Go back to the cluster creation page (create a new cluster), and open the **Worker Type** and/or **Driver Type** dropdowns, and notice how the option to attach to a pool is presented.
4. Clean up by deleting the pool.

## Manage clusters

With the cluster created, go back to the **Compute** page to view the cluster(s). Remind learners that as their organizations grow so will the list, and so you can mention the options to filter, search and sort the list.

Select a cluster to adjust configuration. Click the **Edit** button and show how settings can be modified. Call out that some settings may require running clusters to be restarted.

Though we don't get heavy into permissions, show the **Permissions** dialog and highlight the fact that we can control which users can access the cluster and what they can do with it, referring back to the Access Control table in the slides.

Show the **Clone** funcitonality, which allows us to quickly rubberstamp cluster configurations. Also highlight the run controls and **Delete** button.

Show the **Libraries** tab, which allows us to install addtional libraries (jar files) onto the cluster. These are used to install drivers or libraries needed by workloads

Walk through the various logs.
