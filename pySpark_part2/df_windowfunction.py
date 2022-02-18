from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, array_contains, sum, avg, max, lit, when, expr, concat, concat_ws, date_format, date_add, date_sub, datediff, add_months
from pyspark.sql.functions import approx_count_distinct, collect_list, collect_set, countDistinct
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, lag, lead, avg, sum,min, max

# this is methord is not recomanded one but you can use it for quick testing purpose

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName('Window Function')\
        .master('local[*]')\
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

    columns = ["employee_name", "department", "salary"]
    win_df = spark.createDataFrame(data=simpleData, schema=columns)
    win_df.printSchema()
    win_df.show(truncate=False)

    #1st step is to create the window
    windowSpec = Window.partitionBy("department").orderBy("salary")


    rnoDf = win_df.withColumn('Rno', row_number().over(windowSpec))
    #rnoDf.show()

    rankDF =win_df.withColumn('Rank', rank().over(windowSpec))
    rankDF.show()

    densrankDF =win_df.withColumn('Dense_Rank', dense_rank().over(windowSpec))
    densrankDF.show()

    ntileDF =win_df.withColumn('ntile', ntile(2).over(windowSpec))
    ntileDF.show()

    #leadDF =win_df.withColumn('lead', lead().over(windowSpec))
    #leadDF.show()

    #lagDF =win_df.withColumn('lag', lag().over(windowSpec))
    #lagDF.show()


    windowSpecAgg = Window.partitionBy('department')

    win_df.withColumn('row', row_number().over(windowSpec))\
        .withColumn('avg', avg(col('salary')).over(windowSpecAgg)) \
        .withColumn('sum', sum(col('salary')).over(windowSpecAgg)) \
        .withColumn('min', min(col('salary')).over(windowSpecAgg)) \
        .withColumn('max', max(col('salary')).over(windowSpecAgg)) \
        .where(col('row')==1).select('department', 'avg','max','min', 'sum') \
        .show()