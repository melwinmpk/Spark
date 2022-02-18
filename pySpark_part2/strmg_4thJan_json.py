from pyspark.sql import SparkSession
from  pyspark.sql.functions import col, explode, split, from_json
from pyspark.sql.types import StructType,StructField,StringType



if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Read WebUrl Data') \
        .master('local[*]') \
        .getOrCreate()
    lines_df = spark.readStream\
                .format("socket")\
                .option("host","localhost")\
                .option("port","9999")\
                .load()
    lines_df.printSchema()

    schema = StructType([\
                StructField("device_id", StringType(),True), \
        StructField("device_name", StringType(), True), \
        StructField("humidity", StringType(), True), \
        StructField("lat", StringType(), True), \
        StructField("long", StringType(), True), \
        StructField("scale", StringType(), True), \
        StructField("temp", StringType(), True), \
        StructField("timestamp", StringType(), True), \
        StructField("zipcode", StringType(), True) \
        ])

    jsonDF = lines_df.select(from_json(col("value"), schema).alias("New_Cols")) \
            .select("New_Cols.*")

    word_count_query = jsonDF.writeStream \
                        .format("console") \
                        .outputMode("append") \
                        .option("truncate","False")\
                        .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir6") \
                        .start()

    word_count_query.awaitTermination()