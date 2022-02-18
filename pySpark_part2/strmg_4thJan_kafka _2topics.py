from pyspark.sql import  SparkSession
from pyspark.sql.functions import col, split, explode, from_json, expr
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, DoubleType, ArrayType
############################################################
#        producer -> topic1 -> ------------------> pyspark Stream-------- -> topic 2 -> consumer
#
#        getting the data from one topic and sending the data to another topic via pyspark Stream
#          while sending pyspark Stream converts it to the key value pare
###################################################################
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
        .option("subscribe", "myInvoice") \
        .option("startingOffsets", "latest") \
        .load()

    value_df = kafka_df.select(from_json(col('value').cast('string'), schema).alias('value'))  # .select("value.*")
    value_df.printSchema()

    notification_df = value_df.select("value.InvoiceNumber","value.CustomerCardNo","value.TotalAmount")\
                        .withColumn("EarnedLoyalityPoints",expr("TotalAmount * 0.2 "))
    #notification_df.show(truncate=False)
    notification_df.printSchema()
    #value_df.select("value.*").printSchema()

    # before sending it ot topic we are supposed to convert the data to the key value pare
    kafka_target_df = notification_df.selectExpr(
        "InvoiceNumber as key",
        """
            to_json(named_struct('CustomerCardNo',CustomerCardNo,
                                 'TotalAmount',TotalAmount,
                                 'EarnedLoyalityPoints',TotalAmount*0.2)) as value
        """
    )
    kafka_target_df.printSchema()

    # invoice_writer_query = kafka_target_df.writeStream \
    #     .format("console") \
    #     .queryName("Flattened Invoice Writer") \
    #     .outputMode("append") \
    #     .option('truncate', 'False') \
    #     .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir23") \
    #     .start()

    notification_write_query = kafka_target_df.writeStream \
                                .queryName("Notification Writer") \
                                .format('kafka') \
                                .option("kafka.bootstrap.servers","localhost:9092") \
                                .option("topic", "notifications") \
                                .outputMode('append') \
                                .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir23") \
                                .start()
    notification_write_query.awaitTermination()


