from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.types import StructType, StringType, StructField,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Assignment11thJan") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/usdata.csv')
    df1 = df.withColumn('company_name,address', concat_ws("|", df.company_name, df.address)).drop("company_name",
                                                                                                  "address") \
        .withColumn('city,county,state', concat_ws("~", df.city, df.county, df.state)).drop("city", "county", "state") \
        .select("first_name", "last_name", "company_name,address", "city,county,state", "zip", "age", "phone1",
                "phone2", "email", "web")

    dataSchema = StructType([
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('company_name,address', StringType(), True),
        StructField('city,county,state', StringType(), True),
        StructField('zip', IntegerType(), True),
        StructField('age', IntegerType(), True),
        StructField('phone1', StringType(), True),
        StructField('phone2', StringType(), True),
        StructField('email', StringType(), True),
        StructField('web', StringType(), True),
    ])
    newDF = spark.createDataFrame(df1.rdd, schema=dataSchema)
    newDF.show(1, truncate=False)
    newDF.printSchema()