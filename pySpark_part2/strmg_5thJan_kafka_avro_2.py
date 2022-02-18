from pyspark.sql import  SparkSession
from pyspark.sql.functions import col, split, explode, from_json, expr, struct, sum, to_json
from pyspark.sql.avro.functions import to_avro, from_avro
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, DoubleType, ArrayType


if __name__ == '__main__':
    spark = SparkSession.builder \
     .appName("Streaming Word Count") \
     .master("local[3]") \
     .getOrCreate()

    # setting the avro schema
    avroSchema = open('/home/saif/LFS/datasets/avro_invoice_items_schema.txt', mode='r').read()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "latest") \
        .load()
    kafka_df.printSchema()

    value_df = kafka_df.select(from_avro(col("value"), avroSchema).alias("value"))
    value_df.printSchema()

    # filter only PRIME getting group by
    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.ItemPrice").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))
    rewards_df.printSchema()

    kafka_target_df = rewards_df.select(expr("CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))
    kafka_target_df.printSchema()
    # kafka_target_df.show(truncate=False)

    invoiceWriterQuery = kafka_target_df.writeStream \
                                .format('kafka') \
                                .option("kafka.bootstrap.servers", "localhost:9092") \
                                .option("topic", "cust-rewards") \
                                .outputMode('update') \
                                .option("truncate", "False") \
                                .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir6") \
                                .start()

    invoiceWriterQuery.awaitTermination()