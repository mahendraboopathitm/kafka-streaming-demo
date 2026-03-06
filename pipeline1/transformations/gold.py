import dlt
from pyspark.sql.functions import (
    count, avg, min, max, countDistinct,
    desc, current_timestamp
)

# Gold 1 — Loyalty Tier Summary
@dlt.table(
    name="boopathiad.gold_customers.gold_loyalty_table",
    comment="Customer count and avg tenure by loyalty tier",
    table_properties={"quality": "gold"}
    # ✅ Stores Delta files in YOUR gold container

)
def gold_loyalty_summary():
    return (
        dlt.read("boopathiad.silver_customers.silver_table")
        .groupBy("loyalty_tier", "loyalty_rank")
        .agg(
            count("customer_id").alias("total_customers"),
            avg("customer_tenure_days").alias("avg_tenure_days"),
            min("signup_date").alias("earliest_signup"),
            max("signup_date").alias("latest_signup")
        )
        .withColumn("updated_at", current_timestamp())
    )


# Gold 2 — State Distribution
@dlt.table(
    name="boopathiad.gold_customers.gold_state_table",
    comment="Customer distribution across states",
    table_properties={"quality": "gold"}
    # ✅ Stores Delta files in YOUR gold container
)
def gold_state_summary():
    return (
        dlt.read("boopathiad.silver_customers.silver_table")
        .groupBy("state", "country")
        .agg(
            count("customer_id").alias("total_customers"),
            countDistinct("city").alias("total_cities")
        )
        .withColumn("updated_at", current_timestamp())
    )


# Gold 3 — Yearly Signup Trends
@dlt.table(
    name="boopathiad.gold_customers.gold_signup_table",
    comment="Customer signups by year and loyalty tier",
    table_properties={"quality": "gold"}
    # ✅ Stores Delta files in YOUR gold container

)
def gold_signup_trends():
    return (
        dlt.read("boopathiad.silver_customers.silver_table")
        .groupBy("signup_year", "loyalty_tier", "loyalty_rank")
        .agg(
            count("customer_id").alias("new_customers"),
            avg("customer_tenure_days").alias("avg_tenure_days")
        )
        .withColumn("updated_at", current_timestamp())
    )