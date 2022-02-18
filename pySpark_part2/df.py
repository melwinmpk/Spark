from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Creatign Data frame')\
            .master('local[*]').getOrCreate()
    df = spark.read.format('csv')\
        .option('header','True')\
        .option('inferSchema','True')\
        .load('file:///home/saif/LFS/datasets/orders.txt')
    df.show(5,truncate=False)
    df.printSchema()