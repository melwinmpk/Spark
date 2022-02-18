from pyspark.sql import SparkSession
from pyspark.sql.functions import col,year,to_timestamp,month,dayofweek,date_format


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
    df = df.withColumn('txndate', to_timestamp(df["txndate"], 'MM-dd-yyyy'))
    df1 = df.withColumn('year', year(df['txndate'])) \
        .withColumn('month', month(df['txndate'])) \
        .withColumn('day', date_format(df['txndate'], 'EEEE'))
    df2 = df1["txndate", "amount"].groupby('txndate').sum()
    df2.write.option("header", True) \
        .mode("overwrite") \
        .json("hdfs://localhost:9000/user/saif/HFS/Output/df_op/txns_json")