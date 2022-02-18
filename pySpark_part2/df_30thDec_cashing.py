from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Read WebUrl Data') \
        .master('local[*]') \
        .getOrCreate()

    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/txns_big')
    df.show()

    '''
    cacheDf = df.where(col("state")=="California").cache()
    print(cacheDf.count())

    readingCachedDf = cacheDf.where(col("spendby") == "credit")
    print(readingCachedDf.count())
    input("enter somthing to exit")

    '''

    persistDF = df.persist(StorageLevel.DISK_ONLY)
    persistDF.show()

    input("enter !!!")
    df.unpersist()