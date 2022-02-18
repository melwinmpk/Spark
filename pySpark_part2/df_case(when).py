from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit,when,expr

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()

    data = [
            ("Saif", "", "Shaikh", "36636", "M", 60000),
            ("Ram", "Shirali", "", "40288", "M", 70000),
            ("Aniket", "", "Mishra", "42114", "", 400000),
            ("Mitali", "Sahil", "Kashiv", "39192", "F", 500000),
            ("Nahid", "Alim", "Shaikh", "", "F", 0)
            ]

    column = ["first","middle","last","dob","gender","salary"]
    df = spark.createDataFrame(data,column)
    df.show()

    df2 = df.withColumn('new_gender',
                        when(col("gender") == "M", "Male")
                        .when(col("gender") == "F", "Female")
                        .otherwise("Unknown")
                        )
    df2.show(truncate=False)

    df3 = df.withColumn("new_gender",
                        expr(
                            """
                                case when gender = 'M' then 'MALE' 
                                when gender ='F' then 'Female'
                                else 'Unknown' end
                            """
                        )
                        )
    df3.show()