from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()

    orderingData = [("Saif", "Sales", "HYD", 90000, 34, 10000),
                    ("Ram", "Sales", "HYD", 86000, 56, 20000),
                    ("Aniket", "Sales", "MUM", 81000, 30, 23000),
                    ("Saima", "Finance", "MUM", 90000, 24, 23000),
                    ("Sufiyan", "Finance", "MUM", 99000, 40, 24000),
                    ("Alim", "Finance", "HYD", 83000, 36, 19000),
                    ("Mitali", "Finance", "HYD", 79000, 53, 15000),
                    ("Neha", "Marketing", "MUM", 80000, 25, 18000),
                    ("Kajal", "Marketing", "HYD", 91000, 50, 21000)]

    columns = ["employee_name","department","state","salary","age","bonus"]

    df =spark.createDataFrame(orderingData,columns)
    df.show()

    df5 = df.groupBy("department")\
        .agg(sum("salary").alias("sum salary"),\
             avg("salary").alias("avg_salary"),\
             sum("bonus").alias("sum_bonus"),\
             max("bonus").alias("max_bonus")\
              )\
        .show(truncate=False)

