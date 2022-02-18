from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, from_json, expr, to_timestamp, window, sum

if __name__ == '__main__':
    spark = SparkSession.builder \
     .appName("Streaming Word Count") \
     .master("local[3]") \
     .getOrCreate()

    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trades") \
        .option("startingOffsets", "latest") \
        .load()
    kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"), stock_schema).alias("value"))
    value_df.printSchema()
    trade_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
        .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
    trade_df.printSchema()

    #tumbeling window

    window_agg_df = trade_df \
        .groupBy(window(col("CreatedTime"), "15 minute")) \
        .agg(sum("Buy").alias("TotalBuy"),
             sum("Sell").alias("TotalSell"))
    window_agg_df.printSchema()

    output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")

    window_query = output_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir1") \
        .trigger(processingTime="1 minute") \
        .start()
    window_query.awaitTermination()

