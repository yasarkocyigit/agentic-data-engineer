"""
Gold Layer - Operational Metrics (SDP Materialized Views)
==========================================================
Spark Declarative Pipelines (Spark 4.1+)

Operational KPIs for business monitoring.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    max as spark_max, min as spark_min,
    date_trunc, year, month, current_timestamp, lit,
    countDistinct, when, lag, percent_rank
)
from pyspark.sql.window import Window

spark = SparkSession.getActiveSession()


# =============================================================
# MATERIALIZED VIEWS
# =============================================================

@dp.materialized_view(name="monthly_sales_report")
def monthly_sales_report():
    """
    Monthly sales report with MoM growth.
    Used for: Executive dashboards, board reporting
    Grain: year + month
    """
    orders = spark.table("silver_sales_orders")

    monthly = orders.groupBy(
        year(col("order_date")).alias("year"),
        month(col("order_date")).alias("month")
    ).agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("total_due").alias("revenue"),
        spark_sum("subtotal").alias("subtotal"),
        spark_sum("tax_amount").alias("tax"),
        spark_sum("freight").alias("freight"),
        avg("total_due").alias("avg_order_value"),
        spark_max("total_due").alias("largest_order"),
        spark_min("total_due").alias("smallest_order")
    )

    # MoM growth calculation
    window = Window.orderBy("year", "month")
    return monthly \
        .withColumn("prev_revenue", lag("revenue").over(window)) \
        .withColumn("mom_growth_pct",
                     when(col("prev_revenue").isNotNull(),
                          spark_round(
                              (col("revenue") - col("prev_revenue"))
                              / col("prev_revenue") * 100, 2))
                     .otherwise(None)) \
        .withColumn("revenue", spark_round(col("revenue"), 2)) \
        .withColumn("avg_order_value",
                     spark_round(col("avg_order_value"), 2)) \
        .drop("prev_revenue") \
        .withColumn("_gold_created_at", current_timestamp()) \
        .withColumn("_metric_type", lit("monthly_sales"))


@dp.materialized_view(name="territory_ranking")
def territory_ranking():
    """
    Territory ranking with contribution percentages.
    Used for: Regional performance comparison
    Grain: territory
    """
    orders = spark.table("silver_sales_orders")

    territory = orders.groupBy(
        col("territory_id")
    ).agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("total_due").alias("total_revenue"),
        avg("total_due").alias("avg_order_value")
    )

    # Revenue ranking
    window = Window.orderBy(col("total_revenue").desc())
    return territory \
        .withColumn("revenue_rank",
                     count("territory_id").over(
                         Window.orderBy(col("total_revenue").desc())
                         .rowsBetween(Window.unboundedPreceding,
                                      Window.currentRow))) \
        .withColumn("total_revenue",
                     spark_round(col("total_revenue"), 2)) \
        .withColumn("avg_order_value",
                     spark_round(col("avg_order_value"), 2)) \
        .withColumn("revenue_percentile",
                     spark_round(percent_rank().over(
                         Window.orderBy("total_revenue")) * 100, 1)) \
        .withColumn("_gold_created_at", current_timestamp()) \
        .withColumn("_metric_type", lit("territory_ranking"))


@dp.materialized_view(name="order_fulfillment_metrics")
def order_fulfillment_metrics():
    """
    Order fulfillment and processing metrics.
    Used for: Operations monitoring
    Grain: year + month + status
    """
    from pyspark.sql.functions import datediff

    orders = spark.table("silver_sales_orders")

    return orders.groupBy(
        year(col("order_date")).alias("year"),
        month(col("order_date")).alias("month"),
        col("status")
    ).agg(
        count("order_id").alias("order_count"),
        spark_sum("total_due").alias("total_value"),
        avg("total_due").alias("avg_value")
    ).withColumn("total_value", spark_round(col("total_value"), 2)) \
     .withColumn("avg_value", spark_round(col("avg_value"), 2)) \
     .withColumn("_gold_created_at", current_timestamp()) \
     .withColumn("_metric_type", lit("order_fulfillment"))
