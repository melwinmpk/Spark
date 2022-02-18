from pyspark.sql import SparkSession
from  pyspark.sql.functions import col, explode, split, from_json
from pyspark.sql.types import StructType,StructField,StringType, LongType, DoubleType, IntegerType, ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Streaming Kafka as Source ') \
        .master("local[3]") \
        .getOrCreate()

    kafka_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","myInvoice")\
        .option("startingOffsets","earliest")\
        .load()
    kafka_df.printSchema()

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

    value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value")).select("value.*")
    value_df.printSchema()

    invoiceWriterQuery = value_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate","False") \
        .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir10") \
        .start()

    invoiceWriterQuery.awaitTermination()