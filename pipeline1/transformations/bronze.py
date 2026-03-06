import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="boopathiad.bronze_customers.bronze_table",
    comment="Raw customer data landed from ADLS raw container",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ",")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",
                "abfss://bronze@boopathistorageaccount.dfs.core.windows.net/_schema_v2")
        # ✅ Reads new files from RAW container
        .load("abfss://raw@boopathistorageaccount.dfs.core.windows.net/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )