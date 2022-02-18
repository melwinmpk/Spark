from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from urllib.request import urlopen
from pyspark.sql.functions import explode



if __name__=='__main__':
    SparkConf = SparkConf()
    sc=SparkContext(conf=SparkConf)
    spark = SparkSession.builder\
        .appName('Read WebUrl Data')\
        .master('local[*]')\
        .getOrCreate()

    readFile = urlopen("https://randomuser.me/api/0.8/?results=10").read().decode('utf-8')
    #without utf-8 it give output in binary format
    #print(readFile)
    myDf =spark.read.json(sc.parallelize([readFile]), multiLine=True)
    #myDf.printSchema()
    #myDf.show()

    extract_fields=myDf.withColumn('results', explode('results'))
    extract_fields.printSchema()
    #extract_fields.show()

    flattenDf = extract_fields.select("nationality", \
                                      "results.user.cell", "results.user.dob", "results.user.email", "results.user.gender",\
                                      "results.user.NINO","results.user.cell","results.user.dob","results.user.picture", \
                                      "results.user.md5","results.user.name","results.user.password","results.user.phone"\
                                      )
    flattenDf.show(truncate=False)
    flattenDf.printSchema()