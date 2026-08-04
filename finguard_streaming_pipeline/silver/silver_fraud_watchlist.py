from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F


@dp.table(
    name="finguard.silver.fraud_watchlist",
    comment="Cleaned fraud watchlist data from bronze layer"
)
def silver_fraud_watchlist() -> DataFrame :
    
    df = spark.readStream.table("finguard.bronze.fraud_watchlist")
    df_transformed = (
        df.withColumn("watchlist_id", F.upper(F.col("watchlist_id")))
          .withColumn("risk_level", F.upper(F.col("risk_level")))
          .withColumn("action", F.upper(F.col("action")))
          .withColumn("effective_from", F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss"))
          .withColumnRenamed("ingestion_timestamp", "bronze_ingestion_timestamp")
          .withColumn("silver_ingestion_timestamp", current_timestamp())
    )
    return df_transformed
    