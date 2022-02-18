from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col, array_contains
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()

    arrayData = [
            (("Saif", "", "Shaikh"), ["English", "Science", "Maths"], "HYD", "M"),
            (("Ram", "Sachin", ""), ["Spark", "English", "Maths"], "BLR", "F"),
            (("Aniket", "", "Mishra"), ["Civics", "History"], "HYD", "F"),
            (("Mitali", "Sahil", "Kashiv"), ["Civics", "History"], "BLR", "M"),
            (("Zaid", "Riyaz", "Shaikh"), ["Civics", "History"], "BLR", "M"),
            (("Sufi", "Alim", "Shaikh"), ["Hindi", "History"], "HYD", "M")]

    arrayStructureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('languages', ArrayType(StringType()), True),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])
    df = spark.createDataFrame(arrayData,arrayStructureSchema)
    df.show(truncate=False)
    df.printSchema()

    df1 = df.filter(df.state == 'HYD')
    df1.show(truncate=False)
    df2 = df.filter(col("state") == 'HYD')
    df2.show(truncate=False)
    df3 = df.filter("state == 'HYD'")
    df3.show(truncate=False)

    df4 = df.filter((df.state == "HYD") & (df.gender == "M"))
    df4.show(truncate=False)

    # ARRAY COLUMN FILTER
    df5 = df.filter(array_contains(df.languages, "Science"))
    df5.show(truncate=False)

    # Struct Condition

