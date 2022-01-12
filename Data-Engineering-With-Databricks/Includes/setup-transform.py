# Databricks notebook source
# MAGIC %run ./setup-clean

# COMMAND ----------

def create_transactions():
  spark.sql(f"""
    SELECT * FROM (
      SELECT
        user_id,
        order_id,
        transaction_timestamp,
        total_item_quantity,
        purchase_revenue_in_usd,
        unique_items,
        a.items_exploded.item_id item_id,
        a.items_exploded.quantity quantity
      FROM
        ( SELECT *, explode(items) items_exploded FROM sales ) a
        INNER JOIN users b 
        ON a.email = b.email
    ) PIVOT (
      sum(quantity) FOR item_id in (
        'P_FOAM_K',
        'M_STAN_Q',
        'P_FOAM_S',
        'M_PREM_Q',
        'M_STAN_F',
        'M_STAN_T',
        'M_PREM_K',
        'M_PREM_F',
        'M_STAN_K',
        'M_PREM_T',
        'P_DOWN_S',
        'P_DOWN_K'
      )
    )
  """).createOrReplaceTempView("transactions")

if mode != "clean":
  create_transactions()



