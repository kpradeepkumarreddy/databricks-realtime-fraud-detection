from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="finguard.bronze.transactions",
    comment="Raw transactions data ingested from kafka stream"
)
def bronze_transactions() -> DataFrame :
    
    transactions_streaming_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", dbutils.secrets.get("finguard-secrets-scope", "kafka-bootstrap-server")) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", 
                f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{dbutils.secrets.get("finguard-secrets-scope", "kafka-api-key")}" password="{dbutils.secrets.get("finguard-secrets-scope", "kafka-api-secret")}";') \
        .option("subscribe", dbutils.secrets.get("finguard-secrets-scope", "kafka-topic-name")) \
        .option("startingOffsets", "earliest") \
        .load()

    return transactions_streaming_df \
        .withColumn("key", transactions_streaming_df["key"].cast("string")) \
        .withColumn("value", transactions_streaming_df["value"].cast("string")) \
        .withColumn("ingestion_timestamp", current_timestamp())