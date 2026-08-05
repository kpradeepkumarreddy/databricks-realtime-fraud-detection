from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="finguard.gold.fraud_transactions",
    comment="Detecting fraud transactions by joining transactions table with fraud watchlist table"
)
def fraud_transactions() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions") \
        .withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist = spark.readStream.table("finguard.silver.fraud_watchlist") \
        .withWatermark("effective_from", "5 minutes")
    customers = spark.read.table("finguard.silver.customers")

    joined_stream = transactions.join(
        fraud_watchlist,
        transactions.card_number == fraud_watchlist.entity_id,
        "inner"
    )

    result = joined_stream.join(
        customers,
        joined_stream["customer_id"] == customers["customer_id"],
        "inner"
    ).select(
        # Alert identification
        F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
        F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
        F.current_timestamp().alias("alert_timestamp"),

        # Transaction details
        F.col("transaction_id"),
        transactions["customer_id"],
        customers.email.alias("customer_email"),
        F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
        transactions["card_number"],
        F.col("amount"),
        F.col("currency"),
        F.col("merchant_id"),
        F.col("merchant_name"),
        F.col("merchant_category"),
        F.col("transaction_type"),
        F.col("payment_channel"),
        F.col("device_id"),
        transactions["city"].alias("transaction_city"),
        transactions["country"].alias("transaction_country"),
        F.col("transaction_timestamp"),
        F.col("is_international"),
        transactions["status"].alias("transaction_status"),

        # Fraud watchlist details
        F.col("watchlist_id"),
        F.col("watch_type"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("effective_from").alias("watchlist_effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("fraud_watchlist.city").alias("watchlist_city"),
        F.col("fraud_watchlist.country").alias("watchlist_country")
    )

    return result