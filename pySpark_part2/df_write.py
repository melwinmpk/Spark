from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Different write Modes')\
            .master('local[*]')\
            .getOrCreate()

    df = spark.read.format('csv')\
         .options(header='True', inferSchema='true', delimiter=',')\
         .load('file:///home/saif/LFS/datasets/movies.csv')
    df.show(5, truncate=False)
    df.printSchema()

    # overwrite, append, error,
    df.write.format('csv')\
        .mode('append')\
        .save('hdfs://localhost:9000/user/saif/HFS/Output/c8_saveMode')
    print('********Data Written *********************')

