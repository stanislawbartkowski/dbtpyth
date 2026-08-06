"""
Python (Spark DataFrame) model.

dbt's Python model support lets a model be defined as a `model(dbt, session)`
function instead of SQL. On the Spark adapter, `session` is a SparkSession,
`dbt.ref(...)` / `dbt.source(...)` return Spark DataFrames, and whatever
DataFrame is returned from `model()` is written out as the model's table --
same DAG semantics as a SQL model, just expressed with the PySpark
DataFrame API.
"""

from pyspark.sql import functions as F


def model(dbt, session):
    dbt.config(materialized="table")

    customers_df = dbt.ref("stg_customers")
    orders_df = dbt.ref("stg_orders")

    order_agg_df = (
        orders_df
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_amount"),
            F.max("order_date").alias("most_recent_order_date"),
        )
    )

    final_df = (
        order_agg_df
        .join(customers_df, on="customer_id", how="left")
        .withColumn(
            "customer_value_segment",
            F.when(F.col("total_amount") >= 300, "high")
             .when(F.col("total_amount") >= 100, "medium")
             .otherwise("low"),
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "order_count",
            "total_amount",
            "most_recent_order_date",
            "customer_value_segment",
        )
    )

    return final_df
