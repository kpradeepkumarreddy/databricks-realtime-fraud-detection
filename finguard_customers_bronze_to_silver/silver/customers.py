from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, col, current_timestamp

@dp.table(
    name = "finguard.silver.customers",
    comment = "parsed and cleaned customers data"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers() -> DataFrame:
    return (
        spark
        .readStream
        .table("finguard.bronze.customers")
        .withColumn("account_open_date", to_date(col("account_open_date"), "yyyy-MM-dd"))
        .withColumn("silver_ingestion_timestamp", current_timestamp())
    )