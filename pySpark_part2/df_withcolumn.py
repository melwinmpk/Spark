from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()
    data = [('Saif', '', 'Shaikh', '1991-04-01', 'M', 3000),
            ('Ram', 'Sachin', '', '2000-05-19', 'M', 4000),
            ('Aniket', '', 'Mishra', '1978-09-05', 'M', 4000),
            ('Mitali', 'Sahil', 'Kashiv', '1967-12-01', 'F', 4000),
            ('Nahid', 'Alim', 'Shaikh', '1980-02-17', 'F', -1)]
    columns = ["firstname","middlename","lastname","dob","gender","salary"]
    df = spark.createDataFrame(data, columns)
    df.show()

    # df = spark.createDataFrame(data=mydata, schema=columnNames)
    df1=df.withColumn('sal',col('salary').cast('Integer'))
    df1.printSchema()

    df2 = df.withColumn('sal', col('salary') * 100)
    df2.show()

    df3 = df.withColumn('country', lit('IND'))
    df3.show()

    df4 = df.withColumnRenamed("gender","New_Gender")
    df4.show(truncate=False)

