from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp,lit,datediff

if __name__ == '__main__':
    spark = SparkSession.builder\
            .appName("DF Assignmetn5thJan")\
            .master("local[*]").getOrCreate()

    df = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'True') \
        .option('inferSchema', 'True') \
        .load('file:///home/saif/LFS/datasets/covid19.txt')

    date_df = df.select(df["D"], df["Y"], to_timestamp(df["DT"], 'dd/MM/yyyy').alias('DT'), df["Wkd"],
                        to_timestamp(df["CM"], 'dd/MM/yyyy').alias('CM'), df["C"], df["Com"], df["TM"], df["M"],
                        df["V"],
                        df["CL"])

    date_df.withColumn("no_of_days", datediff(date_df["CM"], date_df["DT"]))

    date_df.write.option("header", True) \
        .partitionBy("Y") \
        .mode("overwrite") \
        .format("com.databricks.spark.avro") \
        .save("hdfs://localhost:9000/user/saif/HFS/Output/df_op/covid18_parquet")