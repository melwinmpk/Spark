from pyspark.sql import SparkSession
from  pyspark.sql.functions import col, explode, split, from_json
from pyspark.sql.types import StructType,StructField,StringType, LongType, DoubleType, IntegerType, ArrayType



if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Read WebUrl Data') \
        .master('local[*]') \
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
    raw_df = spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("path", "/home/saif/LFS/datasets/streaming/") \
            .load()
    raw_df.printSchema()

    invoiceWriterQuery = raw_df.writeStream \
                            .format("console") \
                            .outputMode("append") \
                            .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir8") \
                            .start()

    invoiceWriterQuery.awaitTermination()