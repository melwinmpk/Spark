from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from urllib.request import urlopen
from pyspark.sql.functions import explode

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)


    spark = SparkSession.builder\
            .appName('Read WebUrl Data')\
            .master('local[*]') \
            .getOrCreate()

    readFile = urlopen("https://randomuser.me/api/0.8/?result=10").read().decode("utf-8")
    print(readFile)
    myDf = spark.read.json(sc.parallelize([readFile]),multiLine=True)
    # myDf.printSchema()
    # myDf.show()

    extract_fields = myDf.withColumn("results",explode("results"))
    extract_fields.printSchema()
    extract_fields.show()
