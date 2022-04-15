-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # SQL for Delta Live Tables
-- MAGIC 
-- MAGIC In the last lesson, we walked through the process of scheduling this notebook as a Delta Live Table (DLT) pipeline. Now we'll explore the contents of this notebook to better understand the syntax used by Delta Live Tables.
-- MAGIC 
-- MAGIC This notebook uses SQL to declare Delta Live Tables that together implement a simple multi-hop architecture based on a Databricks-provided example dataset loaded by default into Databricks workspaces.
-- MAGIC 
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to traditional CTAS statements. DLT tables and views will always be preceded by the **`LIVE`** keyword.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Define tables and views with Delta Live Tables
-- MAGIC * Use SQL to incrementally ingest raw data with Auto Loader
-- MAGIC * Perform incremental reads on Delta tables with SQL
-- MAGIC * Update code and redeploy a pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Bronze Layer Tables
-- MAGIC 
-- MAGIC Below we declare two tables implementing the bronze layer. This represents data in its rawest form, but captured in a format that can be retained indefinitely and queried with the performance and benefits that Delta Lake has to offer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_raw
-- MAGIC 
-- MAGIC **`sales_orders_raw`** ingests JSON data incrementally from the example dataset found in  */databricks-datasets/retail-org/sales_orders/*.
-- MAGIC 
-- MAGIC Incremental processing via <a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> (which uses the same processing model as Structured Streaming), requires the addition of the **`STREAMING`** keyword in the declaration as seen below. The **`cloud_files()`** method enables Auto Loader to be used natively with SQL. This method takes the following positional parameters:
-- MAGIC * The source location, as mentioned above
-- MAGIC * The source data format, which is JSON in this case
-- MAGIC * An arbitrarily sized array of optional reader options. In this case, we set **`cloudFiles.inferColumnTypes`** to **`true`**
-- MAGIC 
-- MAGIC The following declaration also demonstrates the declaration of additional table metadata (a comment and properties in this case) that would be visible to anyone exploring the data catalog.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### customers
-- MAGIC 
-- MAGIC **`customers`** presents CSV customer data found in */databricks-datasets/retail-org/customers/*. This table will soon be used in a join operation to look up customer data based on sales records.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Silver Layer Tables
-- MAGIC 
-- MAGIC Now we declare tables implementing the silver layer. This layer represents a refined copy of data from the bronze layer, with the intention of optimizing downstream applications. At this level we apply operations like data cleansing and enrichment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_cleaned
-- MAGIC 
-- MAGIC Here we declare our first silver table, which enriches the sales transaction data with customer information in addition to implementing quality control by rejecting records with a null order number.
-- MAGIC 
-- MAGIC This declaration introduces a number of new concepts.
-- MAGIC 
-- MAGIC #### Quality Control
-- MAGIC 
-- MAGIC The **`CONSTRAINT`** keyword introduces quality control. Similar in function to a traditional **`WHERE`** clause, **`CONSTRAINT`** integrates with DLT, enabling it to collect metrics on constraint violations. Constraints provide an optional **`ON VIOLATION`** clause, specifying an action to take on records that violate the constraint. The three modes currently supported by DLT include:
-- MAGIC 
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`FAIL UPDATE`** | Pipeline failure when constraint is violated |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | Omitted | Records violating constraints will be included (but violations will be reported in metrics) |
-- MAGIC 
-- MAGIC #### References to DLT Tables and Views
-- MAGIC References to other DLT tables and views will always include the **`live.`** prefix. A target database name will automatically be substituted at runtime, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC 
-- MAGIC #### References to Streaming Tables
-- MAGIC 
-- MAGIC References to streaming DLT tables use the **`STREAM()`**, supplying the table name as an argument.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Gold Table
-- MAGIC 
-- MAGIC At the most refined level of the architecture, we declare a table delivering an aggregation with business value, in this case a collection of sales order data based in a specific region. In aggregating, the report generates counts and totals of orders by date and customer.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Explore Results
-- MAGIC 
-- MAGIC Explore the DAG (Directed Acyclic Graph) representing the entities involved in the pipeline and the relationships between them. Click on each to view a summary, which includes:
-- MAGIC * Run status
-- MAGIC * Metadata summary
-- MAGIC * Schema
-- MAGIC * Data quality metrics
-- MAGIC 
-- MAGIC Refer to this <a href="$./DE 8.3 - Pipeline Results" target="_blank">companion notebook</a> to inspect tables and logs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Update Pipeline
-- MAGIC 
-- MAGIC Uncomment the following cell to declare another gold table. Similar to the previous gold table declaration, this filters for the **`city`** of Chicago. 
-- MAGIC 
-- MAGIC Re-run your pipeline to examine the updated results. 
-- MAGIC 
-- MAGIC Does it run as expected? 
-- MAGIC 
-- MAGIC Can you identify any issues?

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Chicago')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
