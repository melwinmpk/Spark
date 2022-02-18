from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Creating the data frame')\
            .master('local[*]').getOrCreate()
    df = spark.read.format('csv')\
            .option('delimiter','|') \
            .option('header', 'True') \
            .option('inferSchema', 'True') \
            .load('file:///home/saif/LFS/datasets/emp_all.txt')
    df.show(5,truncate=False)
    df.printSchema()