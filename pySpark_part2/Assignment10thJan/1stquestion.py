from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Assignment6thJan") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/txns')
    df1 = df.filter((df.category == "Gymnastics") | (df.category == "Team Sports"))
    df1.show()