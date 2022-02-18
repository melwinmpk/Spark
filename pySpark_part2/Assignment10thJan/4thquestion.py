from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Assignment6thJan") \
        .master("local[*]") \
        .config('spark.jars', '/home/saif/LFS/jars/spark-xml_2.12-0.5.0.jar') \
        .getOrCreate()
    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/txns')

    df1 = df.filter((df.category == "Gymnastics") | (df.category == "Team Sports"))
    print(f"==================FIRST QUESTION ====================================")
    df1.show()

    df2 = df.filter((df.spendby == "credit"))
    print(f"==================SECOND QUESTION ====================================")
    df2.show()
    cond = [df1.txnno == df2.txnno]

    df3 = df1.join(df2, cond, 'inner').select(
        df1["txnno"], df1["txndate"], df1["custno"], df1["amount"], df1["category"],
        df1["product"], df1["city"], df1["state"], df1["spendby"]
    )
    print(f"==================THIRD QUESTION ====================================")
    df3.show()
    print(f"==================FOURTH QUESTION ====================================")
    df3.write.format("com.databricks.spark.xml") \
        .mode("overwrite") \
        .option("rootTag", "txn") \
        .option("rowTag", "records") \
        .save("hdfs://localhost:9000/user/saif/HFS/Input/txns_xml")
    print(f"Writing to the HDFS Done")