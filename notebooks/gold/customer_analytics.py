"""
Gold Layer - Customer Analytics (SDP Materialized Views)
=========================================================
Spark Declarative Pipelines (Spark 4.1+)

Customer-centric KPIs and analytics.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    max as spark_max, min as spark_min,
    datediff, current_date, current_timestamp, lit,
    countDistinct, when, percentile_approx
)

spark = SparkSession.getActiveSession()


# =============================================================
# MATERIALIZED VIEWS
# =============================================================

@dp.materialized_view(name="customer_lifetime_value")
def customer_lifetime_value():
    """
    Customer Lifetime Value (CLV) calculation.
    Used for: Customer segmentation, marketing ROI
    Grain: customer
    """
    orders = spark.table("silver_sales_orders")
    customers = spark.table("silver_customers")

    # Filter SCD2 to current records
    if "_is_current" in customers.columns:
        customers = customers.filter(col("_is_current") == True)

    # Order-level metrics per customer
    customer_orders = orders.groupBy(
        col("customer_id")
    ).agg(
        count("order_id").alias("total_orders"),
        spark_sum("total_due").alias("total_spend"),
        avg("total_due").alias("avg_order_value"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
        countDistinct("territory_id").alias("territories_ordered_from")
    )

    # Calculate CLV metrics
    clv = customer_orders \
        .withColumn("customer_tenure_days",
                     datediff(col("last_order_date"),
                              col("first_order_date"))) \
        .withColumn("avg_days_between_orders",
                     when(col("total_orders") > 1,
                          col("customer_tenure_days") / (col("total_orders") - 1))
                     .otherwise(0)) \
        .withColumn("total_spend",
                     spark_round(col("total_spend"), 2)) \
        .withColumn("avg_order_value",
                     spark_round(col("avg_order_value"), 2)) \
        .withColumn("clv_segment",
                     when(col("total_spend") > 50000, "platinum")
                     .when(col("total_spend") > 20000, "gold")
                     .when(col("total_spend") > 5000, "silver")
                     .otherwise("bronze"))

    # Join with customer dimension for demographics
    result = clv.alias("c").join(
        customers.select("customer_id", "territory_id").alias("d"),
        col("c.customer_id") == col("d.customer_id"),
        "left"
    ).select(
        col("c.customer_id"),
        col("c.total_orders"),
        col("c.total_spend"),
        col("c.avg_order_value"),
        col("c.first_order_date"),
        col("c.last_order_date"),
        col("c.customer_tenure_days"),
        col("c.avg_days_between_orders"),
        col("c.territories_ordered_from"),
        col("c.clv_segment"),
        col("d.territory_id"),
        current_timestamp().alias("_gold_created_at"),
        lit("customer_lifetime_value").alias("_metric_type")
    )

    return result


@dp.materialized_view(name="customer_cohort_analysis")
def customer_cohort_analysis():
    """
    Customer cohort analysis by first-order month.
    Used for: Retention analysis, growth tracking
    Grain: cohort_month + order_month
    """
    from pyspark.sql.functions import date_trunc

    orders = spark.table("silver_sales_orders")

    # Get first order date per customer
    first_orders = orders.groupBy("customer_id").agg(
        spark_min("order_date").alias("first_order_date")
    )

    # Join to tag each order with cohort
    cohorted = orders.alias("o").join(
        first_orders.alias("f"),
        col("o.customer_id") == col("f.customer_id"),
        "inner"
    ).select(
        col("o.customer_id"),
        col("o.order_id"),
        col("o.total_due"),
        col("o.order_date"),
        date_trunc("month", col("f.first_order_date")).alias("cohort_month"),
        date_trunc("month", col("o.order_date")).alias("order_month")
    )

    # Aggregate by cohort
    return cohorted.groupBy("cohort_month", "order_month").agg(
        countDistinct("customer_id").alias("active_customers"),
        count("order_id").alias("total_orders"),
        spark_sum("total_due").alias("cohort_revenue"),
        avg("total_due").alias("avg_order_value")
    ).withColumn("cohort_revenue", spark_round(col("cohort_revenue"), 2)) \
     .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2)) \
     .withColumn("_gold_created_at", current_timestamp()) \
     .withColumn("_metric_type", lit("customer_cohort"))
