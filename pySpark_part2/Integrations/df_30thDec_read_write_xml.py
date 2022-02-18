from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # conf = SparkConf()
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder\
            .appName('Read & write xml')\
            .master('local[3]')\
            .config('spark.jars','/home/saif/LFS/jars/spark-xml_2.12-0.5.0.jar')\
            .getOrCreate()
    # df = spark.read.format('com.databricks.spark.xml').xml('/home/saif/LFS/datasets/book_details')

    df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "catalog") \
        .option("rowTag", "book") \
        .load("/home/saif/LFS/datasets/book_details")

    df.show(truncate=False)

    df.write.format("com.databricks.spark.xml")\
    .option("rootTag", "catalog")\
    .option("rowTag", "book")\
    .save("/home/saif/LFS/datasets/book_details_3")

