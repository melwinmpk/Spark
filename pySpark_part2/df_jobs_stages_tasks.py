from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName('Jobs Stages Tasks')\
            .master('local[*]')\
            .getOrCreate()
    mdicalDF = spark.read.format('csv')\
                .option('header','true')\
                .option('inferSchema','true')\
                .load('file:///home/saif/LFS/datasets/medicalData.csv')

    input('Dummy')