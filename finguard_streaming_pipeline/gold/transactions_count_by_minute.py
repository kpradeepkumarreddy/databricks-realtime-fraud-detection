from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finguard.gold.transactions_count_by_minute",
    comment="Number of transactions per minute"
)
def transactions_count_by_minute() -> DataFrame:
    return (
        spark.readStream.table("finguard.silver.transactions")
        .withWatermark("transaction_timestamp", "5 minutes")
        .groupBy(F.window("transaction_timestamp", "1 minute"))
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("count").alias("transactions_count")
        )
    )