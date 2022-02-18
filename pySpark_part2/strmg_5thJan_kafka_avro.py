from pyspark.sql import  SparkSession
from pyspark.sql.functions import col, split, explode, from_json, expr, struct
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, DoubleType, ArrayType

# frist topics to Json
# structured streaming to second topic in avro format
# from second topics to another structured streaming
# from structured streaming convert to key value store it in another topic \
# (which is continued in the part 2 file strmg_5thJan_kafka_avro_2.py)

if __name__ == '__main__':
    spark = SparkSession.builder \
     .appName("Streaming Word Count") \
     .master("local[3]") \
     .getOrCreate()

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice") \
        .option("startingOffsets", "latest") \
        .load()

    value_df = kafka_df.select(from_json(col('value').cast('string'), schema).alias('value'))  # .select("value.*")
    value_df.printSchema()

    notification_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID", "value.PosID", "value.CustomerType", "value.CustomerCardNo",\
    "value.DeliveryType", "value.DeliveryAddress.City", "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode","explode(value.InvoiceLineItems) as LineItem") \

    notification_df.printSchema()

    flattened_df = notification_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    flattened_df.printSchema()

    # this is another method of the key value pare
    # remember when its in avro you cannot see in readable format

    kafka_target_df = flattened_df.select(expr("InvoiceNumber as key"),
                                          to_avro(struct("*")).alias("value"))
    kafka_target_df.printSchema()
    # kafka_df.printSchema()
    # Linking it to the another topic
    invoiceWriterQuery = kafka_target_df.writeStream \
                            .format('kafka') \
                            .option("kafka.bootstrap.servers", "localhost:9092") \
                            .option("topic", "invoice-items") \
                            .outputMode('append') \
                            .option("truncate","False") \
                            .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir7") \
                            .start()

    invoiceWriterQuery.awaitTermination()