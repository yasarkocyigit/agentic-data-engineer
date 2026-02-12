"""
Gold Layer - Revenue Analytics (SDP Materialized Views)
========================================================
Spark Declarative Pipelines (Spark 4.1+)

This module defines materialized views for revenue analytics.
SDP handles orchestration, compute, and dependency resolution.

Gold Layer Pattern:
  - Each function returns a DataFrame (SDP writes it as a table)
  - @dp.materialized_view = batch recompute on each run
  - @dp.temporary_view = intermediate computation, not persisted
  - Dependencies auto-resolved from spark.read references
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    max as spark_max, min as spark_min,
    date_trunc, year, month, quarter, dayofweek, when,
    current_timestamp, lit, datediff, countDistinct
)

spark = SparkSession.getActiveSession()


# =============================================================
# TEMPORARY VIEWS (Intermediate, not persisted)
# =============================================================

@dp.temporary_view()
def silver_orders():
    """Base: Read Silver fact_sales_orders"""
    return spark.table("silver_sales_orders")


@dp.temporary_view()
def silver_order_items():
    """Base: Read Silver fact_order_items"""
    return spark.table("silver_sales_order_items")


@dp.temporary_view()
def silver_customers():
    """Base: Read Silver dim_customer (current records only)"""
    df = spark.table("silver_customers")
    # SCD2: filter to current records only
    if "_is_current" in df.columns:
        df = df.filter(col("_is_current") == True)
    return df


@dp.temporary_view()
def silver_products():
    """Base: Read Silver dim_product (current records only)"""
    df = spark.table("silver_products")
    if "_is_current" in df.columns:
        df = df.filter(col("_is_current") == True)
    return df


@dp.temporary_view()
def orders_with_items():
    """Joined: orders + items for product-level analysis"""
    orders = spark.table("silver_orders")
    items = spark.table("silver_order_items")
    return orders.alias("o").join(
        items.alias("i"),
        col("o.order_id") == col("i.order_id"),
        "inner"
    )


# =============================================================
# MATERIALIZED VIEWS (Persisted Gold tables)
# =============================================================

@dp.materialized_view(name="revenue_by_territory")
def revenue_by_territory():
    """
    Revenue aggregated by territory and time period.
    Used for: Regional sales performance dashboards
    Grain: territory + month
    """
    orders = spark.table("silver_orders")

    return orders.groupBy(
        col("territory_id"),
        date_trunc("month", col("order_date")).alias("month"),
        year(col("order_date")).alias("year"),
        quarter(col("order_date")).alias("quarter")
    ).agg(
        count("order_id").alias("total_orders"),
        spark_sum("total_due").alias("total_revenue"),
        spark_sum("subtotal").alias("subtotal"),
        spark_sum("tax_amount").alias("total_tax"),
        spark_sum("freight").alias("total_freight"),
        avg("total_due").alias("avg_order_value"),
        spark_max("total_due").alias("max_order_value"),
        spark_min("total_due").alias("min_order_value")
    ).withColumn("total_revenue", spark_round(col("total_revenue"), 2)) \
     .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2)) \
     .withColumn("_gold_created_at", current_timestamp()) \
     .withColumn("_metric_type", lit("revenue_by_territory"))


@dp.materialized_view(name="daily_sales_summary")
def daily_sales_summary():
    """
    Daily sales summary with order and product metrics.
    Used for: Operational dashboards, trend analysis
    Grain: day
    """
    joined = spark.table("orders_with_items")

    return joined.groupBy(
        date_trunc("day", col("o.order_date")).alias("date")
    ).agg(
        countDistinct("o.order_id").alias("total_orders"),
        count("i.order_line_id").alias("total_items_sold"),
        spark_sum("i.quantity").alias("total_quantity"),
        spark_sum("o.total_due").alias("gross_revenue"),
        spark_sum("i.line_total").alias("product_revenue"),
        avg("o.total_due").alias("avg_order_value")
    ).withColumn("day_of_week", dayofweek(col("date"))) \
     .withColumn("is_weekend", col("day_of_week").isin([1, 7])) \
     .withColumn("gross_revenue", spark_round(col("gross_revenue"), 2)) \
     .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2)) \
     .withColumn("_gold_created_at", current_timestamp()) \
     .withColumn("_metric_type", lit("daily_sales"))


@dp.materialized_view(name="product_performance")
def product_performance():
    """
    Product performance metrics.
    Used for: Product analytics, inventory planning
    Grain: product
    """
    items = spark.table("silver_order_items")

    return items.groupBy(
        col("product_id")
    ).agg(
        count("order_line_id").alias("times_ordered"),
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("line_total").alias("total_revenue"),
        avg("unit_price").alias("avg_unit_price"),
        avg("discount_percent").alias("avg_discount"),
        avg("quantity").alias("avg_quantity_per_order"),
        spark_max("line_total").alias("max_line_value"),
        countDistinct("order_id").alias("unique_orders")
    ).withColumn("revenue_per_unit",
                 spark_round(col("total_revenue") / col("total_quantity_sold"), 2)) \
     .withColumn("total_revenue", spark_round(col("total_revenue"), 2)) \
     .withColumn("avg_discount", spark_round(col("avg_discount") * 100, 2)) \
     .withColumn("_gold_created_at", current_timestamp()) \
     .withColumn("_metric_type", lit("product_performance"))
