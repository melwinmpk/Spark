from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Read & write xml')\
            .master('local[3]')\
            .config('spark.jars','/home/saif/LFS/jars/spark-xml_2.12-0.5.0.jar')\
            .getOrCreate()
    # df = spark.read.format('com.databricks.spark.xml').xml('/home/saif/LFS/datasets/book_details')

    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/txns')

    df1 = df.filter((df.category == "Gymnastics") | (df.category == "Team Sports"))

    df2 = df.filter((df.spendby == "credit"))

    cond = [df1.txnno == df2.txnno, df1.txndate == df2.txndate, df1.custno == df2.custno, df1.amount == df2.amount,
            df1.category == df2.category, df1.product == df2.product, df1.city == df2.city, df1.state == df2.state,
            df1.spendby == df2.spendby]

    df3 = df1.join(df2, cond, 'inner').select(
        df1["txnno"], df1["txndate"], df1["custno"], df1["amount"], df1["category"],
        df1["product"], df1["city"], df1["state"], df1["spendby"]
    )

    df3.write.format("com.databricks.spark.xml") \
        .option("rootTag", "txn") \
        .option("rowTag", "records") \
        .save("hdfs://localhost:9000/user/saif/HFS/Input/txns_xml")


