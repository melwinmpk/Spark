from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Read WebUrl Data') \
        .master('local[*]') \
        .getOrCreate()

    df = spark.read.format('json') \
        .load('file:///home/saif/LFS/datasets/users.json')
    df.show()
    # data is not coming in the proper order
    mySchema = StructType([
        StructField('first_name',StringType(),True),
        StructField('last_name',StringType(),True),
        StructField('company_name',StringType(),True),
        StructField('address', StringType(), True),
        StructField('city', StringType(), True),
        StructField('county', StringType(), True),
        StructField('state', StringType(), True),
        StructField('zip', StringType(), True),
        StructField('age', StringType(), True),
        StructField('phone1', StringType(), True),
        StructField('phone2', StringType(), True),
        StructField('email', StringType(), True),
        StructField('web', StringType(), True)
    ])
    df1 = spark.read.format('json') \
        .schema(mySchema)\
        .load('file:///home/saif/LFS/datasets/users.json')

    df1.show()

    # reading the PArkey file
    print("=====================================Parquet file ==================================")
    df2 = spark.read.format('parquet') \
        .load('file:///home/saif/LFS/datasets/Users.parquet')
    df2.show()

    df3 = df1.union(df2)
    print(df3.count())

