import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from urllib.request import urlopen
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, ArrayType


if __name__=='__main__':
    spark = SparkSession.builder\
        .appName('Read WebUrl Data')\
        .master('local[*]')\
        .getOrCreate()


    dataSchema = StructType([
        StructField('results',
                    ArrayType(
                        StructType([
                            StructField('user',
                                        StructType([
                                            StructField('gender', StringType(), True),
                                            StructField('email', StringType(), True),
                                            StructField('username', StringType(), True),
                                            StructField('password', StringType(), True),
                                            StructField('salt', StringType(), True),
                                            StructField('md5', StringType(), True),
                                            StructField('sha1', StringType(), True),
                                            StructField('sha256', StringType(), True),
                                            StructField('registered', IntegerType(), True),
                                            StructField('dob', IntegerType(), True),
                                            StructField('phone', StringType(), True),
                                            StructField('cell', StringType(), True),
                                            StructField('name',
                                                        StructType([
                                                            StructField('title', StringType(), True),
                                                            StructField('first', StringType(), True),
                                                            StructField('last', StringType(), True)
                                                        ])),
                                            StructField('location',
                                                        StructType([
                                                            StructField('city', StringType(), True),
                                                            StructField('state', StringType(), True),
                                                            StructField('street', StringType(), True),
                                                            StructField('zip', IntegerType(), True)
                                                        ])),
                                            StructField('picture',
                                                        StructType([
                                                            StructField('large', StringType(), True),
                                                            StructField('medium', StringType(), True),
                                                            StructField('thumbnail', StringType(), True)
                                                        ]))
                                        ]))
                        ])
                    )
                    ),
        StructField('nationality', StringType(), True),
        StructField('seed', StringType(), True),
        StructField('version', StringType(), True),
    ])

    df1 = spark.read.json('file:///home/saif/LFS/datasets/randomuser.json', schema=dataSchema)
    df1.show()
    exploaded_df = df1.withColumn('results', explode('results')) \
        .select("results.user.gender", "results.user.email", "results.user.username", "results.user.password",
                "results.user.salt", "results.user.md5", "results.user.sha1", "results.user.sha256",
                "results.user.registered", "results.user.dob", "results.user.phone", "results.user.cell",
                "results.user.name.title", "results.user.name.first", "results.user.name.last",
                "results.user.location.city", "results.user.location.state", "results.user.location.street",
                "results.user.location.zip", "results.user.picture.large", "results.user.picture.medium",
                "results.user.picture.thumbnail", "nationality", "seed", "version")
    exploaded_df.show()

    # Selecting the multiple user
    readFile = urlopen("https://randomuser.me/api/0.8/?results=5").read().decode('utf-8')
    data = json.loads(readFile)
    data
    df2 = spark.createDataFrame(data=[data], schema=dataSchema)

    exploaded_df2 = df2.withColumn('results', explode('results')) \
        .select("results.user.gender", "results.user.email", "results.user.username", "results.user.password",
                "results.user.salt", "results.user.md5", "results.user.sha1", "results.user.sha256",
                "results.user.registered", "results.user.dob", "results.user.phone", "results.user.cell",
                "results.user.name.title", "results.user.name.first", "results.user.name.last",
                "results.user.location.city", "results.user.location.state", "results.user.location.street",
                "results.user.location.zip", "results.user.picture.large", "results.user.picture.medium",
                "results.user.picture.thumbnail", "nationality", "seed", "version")
    exploaded_df2.show()