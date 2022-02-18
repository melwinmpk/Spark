from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()

    simpleData1 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Aniket", "Sales", "MUM", 86000, 56, 20000),
                   ("Ram", "Sales", "PUN", 81000, 30, 23000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000)]

    simpleData2 = [("Saif", "Sales", "MUM", 90000, 34, 10000),
                   ("Mitali", "Finance", "PUN", 90000, 24, 23000),
                   ("Sufiyan", "Finance", "MUM", 79000, 53, 15000),
                   ("Alim", "Marketing", "PUN", 80000, 25, 18000),
                   ("Amit", "Marketing", "MUM", 91000, 50, 21000)]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    df1 = spark.createDataFrame(simpleData1, columns)
    df2 = spark.createDataFrame(simpleData2, columns)

    df1.show()
    df2.show()
    unionDF = df1.union(df2)
    unionDF.show()

    distinctunionDF = df1.union(df2).distinct()
    distinctunionDF.show()

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