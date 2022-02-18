from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, from_json, expr
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, DoubleType, ArrayType

if _name_ == "_main_":
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
        .option("subscribe", "myInvoice ") \
        .option("startingOffsets", "latest") \
        .load()
    #kafka_df.printSchema()

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    value_df.printSchema()

    explode_df = value_df.selectExpr("value.InvoiceNumber", "value.CreatedTime",
                                     "value.StoreID", "value.PosID", "value.CustomerType", "value.PaymentMethod",
                                     "value.DeliveryType", "value.DeliveryAddress.City", "value.DeliveryAddress.State",
                                     "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")


    invoice_writer_query = flattened_df.writeStream \
        .format("console") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option('truncate', 'False')\
        .option("checkpointLocation", "file:///home/saif/Desktop/checkpoint/dir22") \
        .start()
    invoice_writer_query.awaitTermination()