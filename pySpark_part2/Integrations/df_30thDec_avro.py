from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # conf = SparkConf()
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder\
            .appName('Read & write avro')\
            .master('local[3]')\
            .config('spark.jars','/home/saif/LFS/jars/spark-avro_2.12-3.0.1.jar')\
            .getOrCreate()
    df = spark.read.format('avro').load('/home/saif/LFS/datasets/users.avro')

    df.show(truncate=False)