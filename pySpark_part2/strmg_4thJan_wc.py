from pyspark.sql import SparkSession
from  pyspark.sql.functions import col, explode, split



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

    words_df = lines_df.select(explode(split("value", " ")).alias("word"))
    counts_df = words_df.groupby("word").count()
    # update, complete
    word_count_query = counts_df.writeStream \
                        .format("console") \
                        .outputMode("update") \
                        .option("truncate","False")\
                        .option("checkpointLocation", "/home/saif/Desktop/checkpoint/dir7") \
                        .start()

    word_count_query.awaitTermination()