import dlt
from pyspark.sql.functions import (
    col, trim, initcap, concat, lit, to_date,
    year, month, datediff, current_date,
    current_timestamp, when
)

@dlt.table(
    name="boopathiad.silver_customers.silver_table",
    comment="Cleaned and validated customer data",
    table_properties={"quality": "silver"}
    # ✅ Stores Delta files in YOUR silver containe
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%.%'")
@dlt.expect_or_drop("valid_loyalty_tier",
                     "loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')")
def silver_customers():
    df = dlt.read_stream("boopathiad.bronze_customers.bronze_table")

    # ✅ Strip spaces from all column names just in case
    for old_col in df.columns:
        df = df.withColumnRenamed(old_col, old_col.strip())

    return (
        df
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name",  initcap(trim(col("last_name"))))
        # ✅ yyyy-MM-dd matches your CSV date format
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
        .withColumn("signup_year",  year(col("signup_date")))
        .withColumn("signup_month", month(col("signup_date")))
        .withColumn("customer_tenure_days", datediff(current_date(), col("signup_date")))
        .withColumn("loyalty_tier", initcap(trim(col("loyalty_tier"))))
        .withColumn("loyalty_rank",
                    when(col("loyalty_tier") == "Platinum", 1)
                    .when(col("loyalty_tier") == "Gold",     2)
                    .when(col("loyalty_tier") == "Silver",   3)
                    .when(col("loyalty_tier") == "Bronze",   4))
        .withColumn("processed_timestamp", current_timestamp())
        .select(
            "customer_id", "full_name", "first_name", "last_name",
            "email", "city", "state", "country",
            "signup_date", "signup_year", "signup_month",
            "customer_tenure_days", "loyalty_tier", "loyalty_rank",
            "ingestion_timestamp", "processed_timestamp", "source_file"
        )
    )