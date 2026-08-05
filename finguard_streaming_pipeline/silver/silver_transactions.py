from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

@dp.table(
    name="finguard.silver.transactions",
    comment="Parsed and cleaned transactions data"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
def silver_transactions() -> DataFrame :
   
    # Define schema for the JSON in 'value' column
    value_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType())
    ])

    bronze_df = spark.readStream.table("finguard.bronze.transactions")
    parsed_df = bronze_df.withColumn("parsed_value", from_json(col("value"), value_schema))

    return parsed_df.select(
        col("parsed_value.transaction_id").alias("transaction_id"),
        col("parsed_value.customer_id").alias("customer_id"),
        col("parsed_value.card_number").alias("card_number"),
        col("parsed_value.merchant_id").alias("merchant_id"),
        col("parsed_value.merchant_name").alias("merchant_name"),
        col("parsed_value.merchant_category").alias("merchant_category"),
        col("parsed_value.amount").alias("amount"),
        col("parsed_value.currency").alias("currency"),
        col("parsed_value.transaction_type").alias("transaction_type"),
        col("parsed_value.payment_channel").alias("payment_channel"),
        col("parsed_value.device_id").alias("device_id"),
        col("parsed_value.city").alias("city"),
        col("parsed_value.country").alias("country"),
        col("parsed_value.transaction_timestamp").alias("transaction_timestamp"),
        col("parsed_value.is_international").alias("is_international"),
        col("parsed_value.status").alias("status"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
    )
