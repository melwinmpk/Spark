from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode,split

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Creating the data frame')\
            .master('local[*]').getOrCreate()
    df = spark.read.format('csv')\
            .option('delimiter',',') \
            .option('header', 'True') \
            .option('inferSchema', 'True') \
            .load('file:///home/saif/LFS/datasets/movies.csv')
    df.show(truncate=False)
    df.withColumn("genres", explode(split(col("genres"), "\\|"))).show()

    df = df.filter(col('genres')=='Comedy')

    df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/saif/HFS/Output/df_op/movies")
    df.show()
