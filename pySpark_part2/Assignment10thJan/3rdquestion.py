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
    df2 = df.filter((df.spendby == "credit"))
    cond = [df1.txnno == df2.txnno]

    df3 = df1.join(df2, cond, 'inner').select(
        df1["txnno"], df1["txndate"], df1["custno"], df1["amount"], df1["category"],
        df1["product"], df1["city"], df1["state"], df1["spendby"]
    )
    df3.show()