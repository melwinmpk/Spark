from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit,when,expr,current_date,date_add,approx_count_distinct,collect_list,collect_set,countDistinct,sumDistinct

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()
    simpleData = [("Saif", "Sales", 3000),
                  ("Ram", "Sales", 4600),
                  ("Aniket", "Sales", 4100),
                  ("Mitali", "Finance", 3000),
                  ("Saif", "Sales", 3000),
                  ("Sandeep", "Finance", 3300),
                  ("John", "Finance", 3900),
                  ("Jeff", "Marketing", 3000),
                  ("Sagar", "Marketing", 2000),
                  ("Swaroop", "Sales", 4100)]
    schema = ["employee_name", "department", "salary"]
    agg_df = spark.createDataFrame(data=simpleData, schema=schema)
    agg_df.printSchema()
    agg_df.show(truncate=False)

    approxDistince = agg_df.select(approx_count_distinct("salary"))
    approxDistince.show()

    # gets the duplicate records
    agg_df.select(collect_list("salary")).show(truncate=False)

    # gets the distinct records
    agg_df.select(collect_set("salary")).show(truncate=False)

    # gets the countDistinct
    agg_df.select(countDistinct("salary","department")).show(truncate=False)

    #
    agg_df.select(sumDistinct("salary")).show(truncate=False)