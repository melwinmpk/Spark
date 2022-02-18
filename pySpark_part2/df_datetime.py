from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit,when,expr,concat,concat_ws,date_format,current_date,date_add

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Different write Modes') \
        .master('local[*]') \
        .getOrCreate()
    df_1 = spark.createDataFrame([('2021-01-15', '2021-02-15',)], ['start_dt', 'end_dt'])
    df_1.show()
    df_1.printSchema()

    df_2 = df_1.select(df_1.start_dt.cast('date'),df_1.end_dt.cast('date'))
    df_2.show()
    df_2.printSchema()

    df_3 = df_1.select("start_dt","end_dt",date_format("start_dt","dd/MM/yyyy").alias('dt_format')).show()


    df_4 = df_1.select("start_dt","end_dt",current_date().alias("cur_dt")).show()

    df_5 = df_1.select("start_dt","end_dt",date_add('start_dt',2).alias("add_2_days")).show()

    # add_months, datediff (in terms of days), addyears

