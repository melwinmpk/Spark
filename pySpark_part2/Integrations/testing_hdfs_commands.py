import subprocess 
from pyspark.sql import SparkSession
#Execute HDFS Commands
hdfs = "/usr/local/hadoop-env/hadoop-3.2.1/bin/hdfs"
result = subprocess.run([hdfs, "dfs", "-rm -r", "/user/saif/HFS/Input/txn_data/"],
stdout=subprocess.PIPE)
result = subprocess.run([hdfs, "dfs", "-mkdir", "/user/saif/HFS/Input/txn_data/"],
stdout=subprocess.PIPE)
print(result.stdout.decode(("utf=8")))
result = subprocess.run([hdfs, "dfs", "-put", "/LFS/datasets/txns /user/saif/HFS/Input/txn_data/"],
stdout=subprocess.PIPE)


if __name__ == '__main__':
    spark = SparkSession.builder\
    .master("local[3]") \
    .appName("Read-Write Hive External Partition") \
    .config("hive.metastore.uris", "thrift://localhost:9083/") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/") \
    .enableHiveSupport() \
    .getOrCreate()

    txnDf = spark.read.format("csv") \
        .option("header", "true").option("inferSchema", "true") \
        .load("hdfs://localhost:9000/user/saif/HFS/Input/txns")
    txnDf.show(5, truncate=False)

    partitionDf = txnDf.selectExpr("txnno", "txndate", "custno", "amount",
                                   "category", "product", "city", "state", "spendby",
                                   "from_unixtime(unix_timestamp(txndate, 'MM-dd-yyyy'), 'yyyy') as Year",
                                   "from_unixtime(unix_timestamp(txndate, 'MM-dd-yyyy'), 'MM') as Month",
                                   "from_unixtime(unix_timestamp(txndate, 'MM-dd-yyyy'), 'dd') as Day")
    partitionDf.show(truncate=False)
    spark.sql(""" 
     create external table cohert_c8.dyn_prtn_txn (txnno string, txndate string, 
     custno string, amount string, category string, product string, city string, state string, 
    spendby string) 
     partitioned by (year string, month string, day string) row format delimited fields 
    terminated by ',' 
     tblproperties("skip.header.line.count"="1") 
     location 'hdfs://localhost:9000/user/saif/HFS/Input/txn_data/' 
     """)
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.exec.max.dynamic.partitions", "100")
    spark.conf.set("hive.exec.max.dynamic.partitions.pernode", "1000")
    partitionDf.write \
        .partitionBy("Year", "Month", "Day") \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("cohert_c8.dyn_prtn_txn")
    print("***Data written to Hive DB Successfully***")