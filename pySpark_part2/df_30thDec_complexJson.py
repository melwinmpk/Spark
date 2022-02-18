from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StringType, StructField

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Read WebUrl Data') \
        .master('local[*]') \
        .getOrCreate()

    # for the colplex json we need to have .option("multiLine","True") other wise data will be currept
    df = spark.read.format('json') \
        .option("multiLine","True") \
        .load('file:///home/saif/LFS/datasets/batters.json')
    df.show()

    print("+++++++++++++++++++FINAL DATA+++++++++++++++++++++++")
    extract_all_fields = df.withColumn('batters', explode('batters.batter'))\
                            .withColumn('topping', explode('topping'))\
                            .select("id","name","ppu","topping.id","topping.type","batters.id","batters.type")
    extract_all_fields.printSchema()
    extract_all_fields.show(truncate=False)

