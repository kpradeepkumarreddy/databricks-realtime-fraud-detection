from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F


@dp.table(
    name="finguard.bronze.fraud_watchlist",
    comment="Raw fraud watchlist data stream ingested from databricks volume using autoloader"
)
def bronze_fraud_watchlist() -> DataFrame :
    
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/finguard/source/fraud_watchlist/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )